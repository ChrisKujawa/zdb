/*
 * Copyright © 2021 Christopher Kujawa (zelldon91@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zell.zdb.log;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.atomix.raft.storage.log.entry.SerializedApplicationEntry;
import io.camunda.zeebe.logstreams.impl.log.LoggedEventImpl;
import io.camunda.zeebe.protocol.impl.encoding.MsgPackConverter;
import io.camunda.zeebe.protocol.impl.record.RecordMetadata;
import io.camunda.zeebe.protocol.record.RecordType;
import io.zell.zdb.log.records.ApplicationRecord;
import io.zell.zdb.log.records.IndexedRaftLogEntryImpl;
import io.zell.zdb.log.records.PersistedRecord;
import io.zell.zdb.log.records.ProcessInstanceRelatedValue;
import io.zell.zdb.log.records.RaftRecord;
import io.zell.zdb.log.records.Record;
import io.zell.zdb.log.records.old.RecordMetadataBefore83;
import io.zell.zdb.raft.RaftLogReader;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Predicate;
import org.agrona.concurrent.UnsafeBuffer;

public class LogContentReader implements Iterator<PersistedRecord> {

    private static final int PROTOCOL_VERSION_83 = 4;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final RaftLogReader reader;
    private Predicate<PersistedRecord> isInLimit = record -> true;
    private Predicate<ApplicationRecord> applicationRecordFilter = null;
    private PersistedRecord next;

    public LogContentReader(Path logPath) {
        this.reader = LogFactory.newReader(logPath);
    }

    @Override
    public boolean hasNext() {
        if (!reader.hasNext()) {
            return false;
        }

        next = convertToPersistedRecord(reader.next());
        if (!isInLimit.test(next)) {
            return false;
        }

        if (applicationRecordFilter == null) {
            return true;
        }

        return applyFiltering();
    }

    private boolean applyFiltering() {
        var filterMatches = next instanceof ApplicationRecord appRecord
                && applicationRecordFilter.test(appRecord);

        if (filterMatches) {
            return true;
        }

        var foundNext = false;
        while (!foundNext && reader.hasNext()) {
            next = convertToPersistedRecord(reader.next());
            if (!isInLimit.test(next)) {
                break;
            }

            if (next instanceof ApplicationRecord appRecord) {
                foundNext = applicationRecordFilter.test(appRecord);
            }
        }
        return foundNext;
    }

    @Override
    public PersistedRecord next() {
        return next;
    }

    private PersistedRecord convertToPersistedRecord(IndexedRaftLogEntryImpl entry) {
        if (entry.isApplicationEntry()) {
            var applicationEntry = (SerializedApplicationEntry) entry.getApplicationEntry();
            var applicationRecord = new ApplicationRecord(
                    entry.index(), entry.term(),
                    applicationEntry.highestPosition(), applicationEntry.lowestPosition());

            var readBuffer = new UnsafeBuffer(applicationEntry.data());

            var offset = 0;
            do {
                var loggedEvent = new LoggedEventImpl();
                var metadata = new RecordMetadata();
                metadata.reset();

                loggedEvent.wrap(readBuffer, offset);
                loggedEvent.readMetadata(metadata);

                var parsedRecord = readRecord(loggedEvent, metadata);
                applicationRecord.getEntries().add(parsedRecord);

                offset += loggedEvent.getLength();
            } while (offset < readBuffer.capacity());
            return applicationRecord;
        } else {
            return new RaftRecord(entry.index(), entry.term());
        }
    }

    private Record readRecord(LoggedEventImpl loggedEvent, RecordMetadata metadata) {
        var valueJson = MsgPackConverter.convertToJson(
                new UnsafeBuffer(loggedEvent.getValueBuffer(), loggedEvent.getValueOffset(), loggedEvent.getValueLength()));

        JsonNode recordValue;
        ProcessInstanceRelatedValue pInstanceRelatedValue;
        try {
            recordValue = objectMapper.readTree(valueJson);
            pInstanceRelatedValue = objectMapper.readValue(valueJson, ProcessInstanceRelatedValue.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse record value JSON", e);
        }

        if (metadata.getProtocolVersion() >= PROTOCOL_VERSION_83) {
            return new Record(
                    loggedEvent.getPosition(),
                    loggedEvent.getSourceEventPosition(),
                    loggedEvent.getTimestamp(),
                    loggedEvent.getKey(),
                    metadata.getRecordType(),
                    metadata.getValueType(),
                    metadata.getIntent(),
                    metadata.getRejectionType(),
                    metadata.getRejectionReason(),
                    metadata.getRequestId(),
                    metadata.getRequestStreamId(),
                    metadata.getProtocolVersion(),
                    metadata.getBrokerVersion().toString(),
                    metadata.getRecordVersion(),
                    metadata.getAuthorization().getAuthData().toString(),
                    recordValue,
                    pInstanceRelatedValue);
        } else {
            var recordMetadataBefore83 = new RecordMetadataBefore83();
            loggedEvent.readMetadata(recordMetadataBefore83);

            return new Record(
                    loggedEvent.getPosition(),
                    loggedEvent.getSourceEventPosition(),
                    loggedEvent.getTimestamp(),
                    loggedEvent.getKey(),
                    recordMetadataBefore83.getRecordType(),
                    recordMetadataBefore83.getValueType(),
                    recordMetadataBefore83.getIntent(),
                    recordMetadataBefore83.getRejectionType(),
                    recordMetadataBefore83.getRejectionReason(),
                    recordMetadataBefore83.getRequestId(),
                    recordMetadataBefore83.getRequestStreamId(),
                    recordMetadataBefore83.getProtocolVersion(),
                    recordMetadataBefore83.getBrokerVersion().toString(),
                    0,
                    "",
                    recordValue,
                    pInstanceRelatedValue);
        }
    }

    public LogContent readAll() {
        var logContent = new LogContent();
        while (hasNext()) {
            logContent.getRecords().add(next());
        }
        return logContent;
    }

    public void seekToPosition(long position) {
        reader.seekToAsqn(position);
    }

    public void seekToIndex(long index) {
        reader.seek(index);
    }

    public void limitToPosition(long toPosition) {
        isInLimit = record ->
                record instanceof RaftRecord
                        || (record instanceof ApplicationRecord appRecord
                        && appRecord.getLowestPosition() < toPosition);
    }

    public void filterForProcessInstance(long instanceKey) {
        applicationRecordFilter = record ->
                record.getEntries().stream()
                        .map(Record::piRelatedValue)
                        .filter(v -> v != null)
                        .filter(v -> v.processInstanceKey() != null)
                        .map(ProcessInstanceRelatedValue::processInstanceKey)
                        .anyMatch(key -> key == instanceKey);
    }

    public void filterForRejections() {
        applicationRecordFilter = record ->
                record.getEntries().stream()
                        .map(Record::recordType)
                        .anyMatch(type -> type == RecordType.COMMAND_REJECTION);
    }
}
