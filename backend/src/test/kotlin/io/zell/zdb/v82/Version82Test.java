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
package io.zell.zdb.v82;

import static io.zell.zdb.TestUtils.TIMESTAMP_REGEX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.db.impl.ZeebeDbConstants;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.protocol.record.value.ErrorType;
import io.zell.zdb.SnapshotFixture;
import io.zell.zdb.SnapshotMetadata;
import io.zell.zdb.ZeebePaths;
import io.zell.zdb.log.LogContentReader;
import io.zell.zdb.log.LogSearch;
import io.zell.zdb.log.LogStatus;
import io.zell.zdb.log.LogWriter;
import io.zell.zdb.log.records.ApplicationRecord;
import io.zell.zdb.log.records.PersistedRecord;
import io.zell.zdb.log.records.RaftRecord;
import io.zell.zdb.log.records.Record;
import io.zell.zdb.state.ZeebeDbReader;
import io.zell.zdb.state.incident.IncidentState;
import io.zell.zdb.state.instance.InstanceState;
import io.zell.zdb.state.process.ProcessState;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.StreamSupport;
import org.agrona.concurrent.UnsafeBuffer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class Version82Test {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Path SNAPSHOT_ZIP = Path.of("src/test/resources/zeebe-states/v8.2.zip");
  private static final String PARTITION = "1";
  private static final long INCIDENT_TASK_ELEMENT_KEY = 2251799813685261L;
  private static SnapshotFixture fixture;
  private static Path snapshotDir;
  private static SnapshotMetadata metadata;

  @BeforeAll
  public static void setup() throws Exception {
    fixture = SnapshotFixture.unzip(SNAPSHOT_ZIP, "v8.2");
    snapshotDir = fixture.snapshotDir();
    metadata = fixture.metadata();
  }

  @AfterAll
  public static void cleanup() throws Exception {
    if (fixture != null) {
      fixture.cleanup();
    }
  }

  private static final BpmnModelInstance PROCESS =
      Bpmn.createExecutableProcess("process")
          .startEvent()
          .parallelGateway("gw")
          .serviceTask("task")
          .zeebeJobType("type")
          .endEvent()
          .moveToLastGateway()
          .serviceTask("incidentTask")
          .zeebeInputExpression("=foo", "bar")
          .zeebeJobType("type")
          .endEvent()
          .done();

  @Nested
  public class ZeebeLogTest {


    @Test
    public void shouldReadStatusFromLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logStatus = new LogStatus(logPath);

      // when
      final var status = logStatus.status();

      // then
      assertThat(status.getHighestIndex()).isEqualTo(15);
      assertThat(status.getHighestTerm()).isEqualTo(1);
      assertThat(status.getHighestRecordPosition()).isEqualTo(62);
      assertThat(status.getLowestIndex()).isEqualTo(1);
      assertThat(status.getLowestRecordPosition()).isEqualTo(1);

      assertThat(status.toString())
          .contains("lowestRecordPosition")
          .contains("highestRecordPosition")
          .contains("highestTerm")
          .contains("highestIndex")
          .contains("lowestIndex");
    }

    @Test
    public void shouldThrowWhenReadStatusFromNonExistingLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(new File("/tmp/doesntExist"), "1");

      // when - throw
      assertThatThrownBy(() -> new LogStatus(logPath))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Expected to read segments, but there was nothing to read");
    }

    @Test
    public void shouldBuildLogContent() throws JsonProcessingException {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);

      // when
      final var content = logContentReader.readAll();

      // then
      verifyCompleteLog(content.getRecords());

      final var objectMapper = new ObjectMapper();
      final var jsonNode = objectMapper.readTree(content.toString());
      assertThat(jsonNode).isNotNull(); // is valid json
    }

    @Test
    public void shouldReadLogContentWithIterator() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      verifyCompleteLog(records);
    }

    @Test
    public void shouldReadRejection() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);

      // when
      logContentReader.filterForRejections();

      // then
      final var rejection =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
              .filter(persistedRecord -> persistedRecord instanceof ApplicationRecord)
              .map(persistedRecord -> (ApplicationRecord) persistedRecord)
              .flatMap(applicationRecord -> applicationRecord.getEntries().stream())
              .filter(record -> record.component8() != RejectionType.NULL_VAL)
              .findFirst();

      assertThat(rejection).isPresent();
      assertThat(rejection.get().component8()).isEqualTo(RejectionType.NOT_FOUND);
      assertThat(rejection.get().component9())
          .isEqualTo(
              "Expected to find process definition with process ID 'nonExisting', but none found");
    }

    @Test
    public void shouldSerializeRejectionToJson() throws JsonProcessingException {
      // given
      final var expectedJson =
          OBJECT_MAPPER.readTree(
"""
                    {"position":62,"sourceRecordPosition":61,"key":-1,"recordType":"COMMAND_REJECTION",
                    "valueType":"PROCESS_INSTANCE_CREATION","intent":"CREATE","rejectionType":"NOT_FOUND",
                    "rejectionReason":"Expected to find process definition with process ID 'nonExisting', but none found",
                    "requestId":-1,"requestStreamId":-2147483648,"protocolVersion":3,"brokerVersion":"8.2.16",
                    "recordValue":{"bpmnProcessId":"nonExisting","processDefinitionKey":0,"processInstanceKey":-1,
                    "version":-1,"variables":"gA==","fetchVariables":[],
                    "startInstructions":[]}}
                    }
""");
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);

      // when
      logContentReader.filterForRejections();

      // then
      final var rejection =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
              .filter(persistedRecord -> persistedRecord instanceof ApplicationRecord)
              .map(persistedRecord -> (ApplicationRecord) persistedRecord)
              .flatMap(applicationRecord -> applicationRecord.getEntries().stream())
              .filter(record -> record.component8() != RejectionType.NULL_VAL)
              .findFirst();

      assertThat(rejection).isPresent();
      assertThat(rejection.get().component8()).isEqualTo(RejectionType.NOT_FOUND);
      assertThat(rejection.get().component9())
          .isEqualTo(
              "Expected to find process definition with process ID 'nonExisting', but none found");

      final var recordJson = rejection.get().toString().replaceFirst(TIMESTAMP_REGEX, "");
      final var actualJson = OBJECT_MAPPER.readTree(recordJson);
      assertThat(actualJson).isNotNull(); // is valid json
      assertThat(actualJson).isEqualTo(expectedJson);
    }

    @Test
    public void shouldSerializeRecordToJson() throws JsonProcessingException {
      // given
      final var expectedJson =
          OBJECT_MAPPER.readTree(
"""
                    {"position":13,"sourceRecordPosition":6,"key":2251799813685252,"recordType":"EVENT",
                    "valueType":"PROCESS_INSTANCE","intent":"ELEMENT_ACTIVATED",
                    "requestId":-1,"requestStreamId":-2147483648,"protocolVersion":3,"brokerVersion":"8.2.16",
                    "recordValue":{"bpmnElementType":"PROCESS","elementId":"process","bpmnProcessId":"process",
                    "version":1,"processDefinitionKey":2251799813685249,"processInstanceKey":2251799813685252,
                    "flowScopeKey":-1,"bpmnEventType":"UNSPECIFIED","parentProcessInstanceKey":-1,
                    "parentElementInstanceKey":-1}}
""");
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      logContentReader.filterForProcessInstance(
          metadata.processInstanceKey());

      // when
      final var piActivated =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
              .filter(persistedRecord -> persistedRecord instanceof ApplicationRecord)
              .map(persistedRecord -> (ApplicationRecord) persistedRecord)
              .flatMap(applicationRecord -> applicationRecord.getEntries().stream())
              .filter(record -> record.component6() == ValueType.PROCESS_INSTANCE)
              .filter(record -> record.component7() == ProcessInstanceIntent.ELEMENT_ACTIVATED)
              .filter(record -> record.getPiRelatedValue() != null)
              .filter(
                  record ->
                      record.getPiRelatedValue().getBpmnElementType() == BpmnElementType.PROCESS)
              .findFirst();

      // then
      assertThat(piActivated).isPresent();
      final var recordJson = piActivated.get().toString().replaceFirst(TIMESTAMP_REGEX, "");
      final var actualJson = OBJECT_MAPPER.readTree(recordJson);
      assertThat(actualJson).isNotNull(); // is valid json
      assertThat(actualJson).isEqualTo(expectedJson);
    }

    @Test
    public void shouldSkipFirstPartOfLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(10);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(11);
      // we skip the first raft record
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(0);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count())
          .isEqualTo(11);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(15);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(5);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(62);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(7);
    }

    @Test
    public void shouldNotSkipIfNegativeSeek() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(-1);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      verifyCompleteLog(records);
    }

    @Test
    public void shouldNotSkipIfZeroSeek() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(0);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      verifyCompleteLog(records);
    }

    @Test
    public void shouldSeekToEndOfLogIfNoExistingSeek() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(Long.MAX_VALUE);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(0);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(15);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(15);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(62);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(62);
    }

    @Test
    public void shouldLimitLogToPosition() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.limitToPosition(30);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(5);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(4);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(5);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(1);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(34);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(1);
    }

    @Test
    public void shouldLimitViaPositionExclusive() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.limitToPosition(1);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(1);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(1);
    }

    @Test
    public void shouldConvertRecordToColumn() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.limitToPosition(2);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(2);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);

      final var record = (ApplicationRecord) records.get(1);
      // Index Term RecordType ValueType Intent Position SourceRecordPosition

      final String columnString = record.asColumnString();
      final String[] elements = columnString.trim().split(" ");
      assertThat(elements).hasSize(9); // deployment record skips the last two columns
      assertThat(elements).containsSubsequence("2", "1", "1", "-1");
      // we skip timestamp since it is not reproducible
      assertThat(elements).containsSubsequence("-1", "COMMAND", "DEPLOYMENT", "CREATE");
    }

    @Test
    public void shouldWriteTableHeaderToStreamWhenNoDataFound() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var outputStream = new ByteArrayOutputStream();
      logContentReader.limitToPosition(30);
      logContentReader.seekToPosition(3);
      logContentReader.filterForProcessInstance(2251799813685254L);
      final var logWriter = new LogWriter(outputStream, logContentReader);

      // when
      logWriter.writeAsTable();

      // then
      assertThat(outputStream.toString().trim())
          .isEqualTo(
              "Index Term Position SourceRecordPosition Timestamp Key RecordType ValueType Intent ProcessInstanceKey BPMNElementType");
    }

    @Test
    public void shouldWriteTableToStream() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      final var outputStream = new ByteArrayOutputStream();
      logContentReader.limitToPosition(600);
      logContentReader.seekToPosition(6);
      logContentReader.filterForProcessInstance(2251799813685252L);
      final var logWriter = new LogWriter(outputStream, logContentReader);

      // when
      logWriter.writeAsTable();

      // then
      assertThat(outputStream.toString())
          .startsWith(
              "Index Term Position SourceRecordPosition Timestamp Key RecordType ValueType Intent ProcessInstanceKey BPMNElementType")
          // EQUALs check is hard due to the timestamp
          .contains("2251799813685253 EVENT VARIABLE CREATED 2251799813685252")
          .contains("EVENT PROCESS_INSTANCE ELEMENT_ACTIVATING 2251799813685252 START_EVENT");
    }

    @Test
    public void shouldSeekAndLimitLogWithPosition() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(5);
      logContentReader.limitToPosition(30);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(3);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(0);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(3);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(5);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(3);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(34);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(2);
    }

    @Test
    public void shouldFilterWithProcessInstanceKey() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.filterForProcessInstance(2251799813685252L);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(0);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(5);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(5);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(34);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(7);
    }

    @Test
    public void shouldFilterWithNoExistingProcessInstanceKey() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.filterForProcessInstance(0xCAFE);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(0);
    }

    @Test
    public void shouldFilterWithProcessInstanceKeyAndSetBeginAndEndOfLogPosition() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.filterForProcessInstance(2251799813685252L);
      logContentReader.seekToPosition(5);
      logContentReader.limitToPosition(30);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(0);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(5);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(5);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(34);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(7);
    }

    private static void verifyCompleteLog(final List<PersistedRecord> records) {
      assertThat(records).hasSize(15);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count())
          .isEqualTo(14);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(15);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(1);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(62);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(1);
    }

    @Test
    public void shouldReturnLogContentAsDotFile() throws JsonProcessingException {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);
      final var content = logContentReader.readAll();

      // when
      final var dotFileContent = content.asDotFile();

      // then
      assertThat(dotFileContent).startsWith("digraph log {").endsWith("}");
    }

    @Test
    public void shouldContainNoDuplicatesInLogContent() throws JsonProcessingException {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logContentReader = new LogContentReader(logPath);

      // when
      final var content = logContentReader.readAll();

      // then
      // validate that records are not duplicated in LogContent
      assertThat(content.getRecords())
          .filteredOn(ApplicationRecord.class::isInstance)
          .asInstanceOf(InstanceOfAssertFactories.list(ApplicationRecord.class))
          .flatExtracting(ApplicationRecord::getEntries)
          .extracting(Record::getPosition)
          .doesNotHaveDuplicates();
    }

    @Test
    public void shouldSearchPositionInLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logSearch = new LogSearch(logPath);
      final var position = 1;

      // when
      final Record record = logSearch.searchPosition(position);

      // then
      assertThat(record).isNotNull();
      assertThat(record.getPosition()).isEqualTo(position);
    }

    @Test
    public void shouldReturnNullOnNegPosition() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logSearch = new LogSearch(logPath);

      // when
      final Record record = logSearch.searchPosition(-1);

      // then
      assertThat(record).isNull();
    }

    @Test
    public void shouldReturnNullOnToBigPosition() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logSearch = new LogSearch(logPath);

      // when
      final Record record = logSearch.searchPosition(Long.MAX_VALUE);

      // then
      assertThat(record).isNull();
    }

    @Test
    public void shouldSearchIndexInLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logSearch = new LogSearch(logPath);
      final var index = 7;

      // when
      final var record = logSearch.searchIndex(index);

      // then
      assertThat(record).isNotNull();
    }

    @Test
    public void shouldNotReturnDuplicatesWhenSearchForIndexInLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logSearch = new LogSearch(logPath);
      final var index = 7;

      // when
      final var record = logSearch.searchIndex(index);

      // then
      // validate that records are not duplicated in LogContent
      assertThat(record)
          .asInstanceOf(InstanceOfAssertFactories.type(ApplicationRecord.class))
          .extracting(ApplicationRecord::getEntries)
          .asInstanceOf(InstanceOfAssertFactories.list(Record.class))
          .extracting(Record::getPosition)
          .doesNotHaveDuplicates();
    }

    @Test
    public void shouldReturnNullOnNegIndex() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logSearch = new LogSearch(logPath);

      // when
      final var logContent = logSearch.searchIndex(-1);

      // then
      assertThat(logContent).isNull();
    }

    @Test
    public void shouldReturnNullOnToBigIndex() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
      final var logSearch = new LogSearch(logPath);

      // when
      final var logContent = logSearch.searchIndex(Long.MAX_VALUE);

      // then
      assertThat(logContent).isNull();
    }
  }

  @Nested
  public class ZeebeStateTest {
    @Test
    public void shouldCreateStatsForCompleteState() {
      // given
      final var experimental = new ZeebeDbReader(ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION));

      // when
      final var cfMap = experimental.stateStatistics();

      // then
      assertThat(cfMap)
          .containsEntry(ZbColumnFamilies.JOBS.name(), 1)
          .containsEntry(ZbColumnFamilies.VARIABLES.name(), 3)
          .containsEntry(ZbColumnFamilies.INCIDENTS.name(), 1)
          .containsEntry(ZbColumnFamilies.ELEMENT_INSTANCE_KEY.name(), 3);
    }

    @Test
    public void shouldVisitValuesAsJson() throws JsonProcessingException {
      // given
      final var experimental = new ZeebeDbReader(ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION));
      final var incidents = new ArrayList<String>();
      final ZeebeDbReader.JsonValueVisitor jsonVisitor =
          (cf, k, v) -> {
            if (cf.equals(ZbColumnFamilies.INCIDENTS.name())) {
              incidents.add(v);
            }
          };

      // when
      experimental.visitDBWithJsonValues(jsonVisitor);

      // then
      assertThat(incidents).hasSize(1);
      final var incident = OBJECT_MAPPER.readTree(incidents.get(0)).get("incidentRecord");
      assertThat(incident.get("errorType").asText()).isEqualTo(ErrorType.IO_MAPPING_ERROR.toString());
      assertThat(incident.get("errorMessage").asText())
          .isEqualTo("failed to evaluate expression '{bar:foo}': no variable found for name 'foo'");
      assertThat(incident.get("bpmnProcessId").asText()).isEqualTo("process");
      assertThat(incident.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
      assertThat(incident.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
      assertThat(incident.get("elementId").asText()).isEqualTo("incidentTask");
      assertThat(incident.get("elementInstanceKey").asLong()).isEqualTo(INCIDENT_TASK_ELEMENT_KEY);
      assertThat(incident.get("variableScopeKey").asLong()).isEqualTo(INCIDENT_TASK_ELEMENT_KEY);
      assertThat(incident.get("jobKey").asLong()).isEqualTo(-1L);
    }

    @Test
    public void shouldListProcesses() {
      // given
      final var processState = new ProcessState(ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION));
      final var processes = new HashMap<Long, String>();

      // when
      processState.listProcesses(
          (key, valueJson) ->
              processes.put(
                  new UnsafeBuffer(key).getLong(Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER),
                  valueJson));

      // then
      assertThat(processes).containsKey(metadata.firstProcessKey()).containsKey(metadata.secondProcessKey());
    }

    @Test
    public void shouldGetProcessDetails() throws JsonProcessingException {
      // given
      final var processes = new ArrayList<String>();
      final Path runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION);
      final var processState = new ProcessState(runtimePath);

      // when
      processState.processDetails(metadata.firstProcessKey(), (k, v) -> processes.add(v));

      // then
      assertThat(processes).hasSize(1);
      final var jsonNode = OBJECT_MAPPER.readTree(processes.get(0));
      assertThat(jsonNode.get("bpmnProcessId").asText()).isEqualTo("process");
      assertThat(jsonNode.get("key").asLong()).isEqualTo(metadata.firstProcessKey());
      assertThat(jsonNode.get("resourceName").asText()).isEqualTo("process.bpmn");
      assertThat(jsonNode.get("version").asInt()).isEqualTo(1);
      final var resourceXml = new String(Base64.getDecoder().decode(jsonNode.get("resource").asText()), StandardCharsets.UTF_8);
      assertThat(resourceXml)
          .contains("<process id=\"process\"")
          .contains("id=\"task\"")
          .contains("id=\"incidentTask\"");
    }

    @Test
    public void shouldNotFailOnNonExistingProcess() {
      // given
      final var processes = new ArrayList<String>();

      // when
      final var processState = new ProcessState(ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION));
      processState.processDetails(0xCAFE, (k, v) -> processes.add(v));

      // then
      assertThat(processes).isEmpty();
    }

    @Test
    public void shouldGetProcessInstanceDetails() throws JsonProcessingException {
      // given
      final var processInstanceKey = metadata.processInstanceKey();

      // when
      final var processState = new InstanceState(ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION));
      final var actualInstanceDetails = processState.getInstance(processInstanceKey);

      // then
      assertThat(actualInstanceDetails).isNotNull();
      final var instanceAsJson = OBJECT_MAPPER.readTree(actualInstanceDetails);
      final var elementRecord = instanceAsJson.get("elementRecord");
      assertThat(elementRecord.get("key").asLong()).isEqualTo(processInstanceKey);
      assertThat(elementRecord.get("state").asText()).isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
      assertProcessInstanceRecord(
          elementRecord.get("processInstanceRecord"), "process", BpmnElementType.PROCESS, -1L);
      assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(2);
    }

    @Test
    public void shouldGetElementInstanceDetails() throws JsonProcessingException {
      // given
      final var processInstanceKey = metadata.processInstanceKey();
      final var elementInstanceKey = metadata.elementInstanceKey();

      // when
      final var processState = new InstanceState(ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION));
      final var actualInstanceDetails = processState.getInstance(elementInstanceKey);

      // then
      assertThat(actualInstanceDetails).isNotNull();
      final var instanceAsJson = OBJECT_MAPPER.readTree(actualInstanceDetails);
      final var elementRecord = instanceAsJson.get("elementRecord");
      assertThat(elementRecord.get("key").asLong()).isEqualTo(elementInstanceKey);
      assertThat(elementRecord.get("state").asText()).isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
      assertProcessInstanceRecord(
          elementRecord.get("processInstanceRecord"),
          "task",
          BpmnElementType.SERVICE_TASK,
          processInstanceKey);
      assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(0);
    }

    @Test
    public void shouldListElementInstanceDetails() throws JsonProcessingException {
      // given
      final var processInstanceKey = metadata.processInstanceKey();
      final var elementInstanceKey = metadata.elementInstanceKey();
      final var processState = new InstanceState(ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION));
      final var list = new ArrayList<String>();

      // when
      processState.listInstances((key, valueJson) -> list.add(valueJson));

      // then
      assertThat(list).hasSize(3);

      final var instancesByKey = new HashMap<Long, JsonNode>();
      for (final var instanceJson : list) {
        final var instance = OBJECT_MAPPER.readTree(instanceJson);
        instancesByKey.put(instance.get("elementRecord").get("key").asLong(), instance);
      }

      final var processInstance = instancesByKey.get(processInstanceKey);
      assertThat(processInstance).isNotNull();
      assertThat(processInstance.get("childCount").asInt()).isEqualTo(2);
      assertThat(processInstance.get("elementRecord").get("state").asText())
          .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
      assertProcessInstanceRecord(
          processInstance.get("elementRecord").get("processInstanceRecord"),
          "process",
          BpmnElementType.PROCESS,
          -1L);

      final var incidentTaskInstance = instancesByKey.get(INCIDENT_TASK_ELEMENT_KEY);
      assertThat(incidentTaskInstance).isNotNull();
      assertThat(incidentTaskInstance.get("childCount").asInt()).isEqualTo(0);
      assertThat(incidentTaskInstance.get("elementRecord").get("state").asText())
          .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATING.toString());
      assertProcessInstanceRecord(
          incidentTaskInstance.get("elementRecord").get("processInstanceRecord"),
          "incidentTask",
          BpmnElementType.SERVICE_TASK,
          processInstanceKey);

      final var taskInstance = instancesByKey.get(elementInstanceKey);
      assertThat(taskInstance).isNotNull();
      assertThat(taskInstance.get("childCount").asInt()).isEqualTo(0);
      assertThat(taskInstance.get("elementRecord").get("state").asText())
          .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
      assertProcessInstanceRecord(
          taskInstance.get("elementRecord").get("processInstanceRecord"),
          "task",
          BpmnElementType.SERVICE_TASK,
          processInstanceKey);
    }

    @Test
    public void shouldNotFailOnNonExistingProcessInstance() {
      // when
      final var processState = new InstanceState(ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION));
      final var actualInstanceDetails = processState.getInstance(0xCAFE);

      // then
      assertThat(actualInstanceDetails).isEqualTo("{}");
    }

    @Test
    public void shouldFindInstancesWithPredicate() {
      // given
      final var instanceState = new InstanceState(ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION));
      final var processes = new HashMap<Long, String>();

      // when
      instanceState.listProcessInstances(
          processInstanceRecordDetails -> processInstanceRecordDetails.getBpmnProcessId().equals("process"),
          (key, valueJson) ->
              processes.put(
                  new UnsafeBuffer(key).getLong(Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER),
                  valueJson));

      // then
      assertThat(processes).containsKey(metadata.processInstanceKey());
    }

    @Test
    public void shouldGetIncidentDetails() throws JsonProcessingException {
      // given
      final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION);

      // when
      final var incidentState = new IncidentState(runtimePath);
      final var incidentAsJson = incidentState.incidentDetails(metadata.incidentKey());

      // then
      assertThat(incidentAsJson).isNotNull();
      final var incident = OBJECT_MAPPER.readTree(incidentAsJson).get("incidentRecord");
      assertThat(incident.get("bpmnProcessId").asText()).isEqualTo("process");
      assertThat(incident.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
      assertThat(incident.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
      assertThat(incident.get("elementInstanceKey").asLong()).isEqualTo(INCIDENT_TASK_ELEMENT_KEY);
      assertThat(incident.get("elementId").asText()).isEqualTo("incidentTask");
      assertThat(incident.get("errorMessage").asText())
          .isEqualTo("failed to evaluate expression '{bar:foo}': no variable found for name 'foo'");
      assertThat(incident.get("errorType").asText()).isEqualTo(ErrorType.IO_MAPPING_ERROR.toString());
      assertThat(incident.get("variableScopeKey").asLong()).isEqualTo(INCIDENT_TASK_ELEMENT_KEY);
      assertThat(incident.get("jobKey").asLong()).isEqualTo(-1L);
    }

    @Test
    public void shouldListIncidentDetails() throws JsonProcessingException {
      // given
      final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION);
      final var list = new ArrayList<String>();

      // when
      final var incidentState = new IncidentState(runtimePath);
      incidentState.listIncidents(list::add);

      // then
      assertThat(list).hasSize(1);
      final var incident = OBJECT_MAPPER.readTree(list.get(0));
      assertThat(incident.get("key").asLong()).isEqualTo(metadata.incidentKey());
      final var incidentRecord = incident.get("value").get("incidentRecord");
      assertThat(incidentRecord.get("bpmnProcessId").asText()).isEqualTo("process");
      assertThat(incidentRecord.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
      assertThat(incidentRecord.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
      assertThat(incidentRecord.get("elementInstanceKey").asLong()).isEqualTo(INCIDENT_TASK_ELEMENT_KEY);
      assertThat(incidentRecord.get("elementId").asText()).isEqualTo("incidentTask");
      assertThat(incidentRecord.get("errorMessage").asText())
          .isEqualTo("failed to evaluate expression '{bar:foo}': no variable found for name 'foo'");
      assertThat(incidentRecord.get("errorType").asText()).isEqualTo(ErrorType.IO_MAPPING_ERROR.toString());
      assertThat(incidentRecord.get("variableScopeKey").asLong()).isEqualTo(INCIDENT_TASK_ELEMENT_KEY);
      assertThat(incidentRecord.get("jobKey").asLong()).isEqualTo(-1L);
    }

    private void assertProcessInstanceRecord(
        final JsonNode processInstanceRecord,
        final String elementId,
        final BpmnElementType bpmnElementType,
        final long flowScopeKey) {
      assertThat(processInstanceRecord.get("bpmnProcessId").asText()).isEqualTo("process");
      assertThat(processInstanceRecord.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
      assertThat(processInstanceRecord.get("version").asInt()).isEqualTo(1);
      assertThat(processInstanceRecord.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
      assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo(elementId);
      assertThat(processInstanceRecord.get("bpmnElementType").asText()).isEqualTo(bpmnElementType.name());
      assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1L);
      assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1L);
      assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(flowScopeKey);
    }
  }

}