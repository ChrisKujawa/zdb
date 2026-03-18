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
package io.zell.zdb.log.records;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import java.io.StringWriter;

public class Record {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final long position;
    private final long sourceRecordPosition;
    private final long timestamp;
    private final long key;
    private final RecordType recordType;
    private final ValueType valueType;
    private final Intent intent;
    private final RejectionType rejectionType;
    private final String rejectionReason;
    private final Long requestId;
    private final int requestStreamId;
    private final int protocolVersion;
    private final String brokerVersion;
    private final Integer recordVersion;
    private final String authData;
    private final JsonNode recordValue;
    private final ProcessInstanceRelatedValue piRelatedValue;

    public Record(
            long position,
            long sourceRecordPosition,
            long timestamp,
            long key,
            RecordType recordType,
            ValueType valueType,
            Intent intent,
            RejectionType rejectionType,
            String rejectionReason,
            Long requestId,
            int requestStreamId,
            int protocolVersion,
            String brokerVersion,
            Integer recordVersion,
            String authData,
            JsonNode recordValue,
            ProcessInstanceRelatedValue piRelatedValue) {
        this.position = position;
        this.sourceRecordPosition = sourceRecordPosition;
        this.timestamp = timestamp;
        this.key = key;
        this.recordType = recordType;
        this.valueType = valueType;
        this.intent = intent;
        this.rejectionType = rejectionType;
        this.rejectionReason = rejectionReason;
        this.requestId = requestId;
        this.requestStreamId = requestStreamId;
        this.protocolVersion = protocolVersion;
        this.brokerVersion = brokerVersion;
        this.recordVersion = recordVersion;
        this.authData = authData;
        this.recordValue = recordValue;
        this.piRelatedValue = piRelatedValue;
    }

    public long getPosition() {
        return position;
    }

    public long getSourceRecordPosition() {
        return sourceRecordPosition;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getKey() {
        return key;
    }

    public RecordType getRecordType() {
        return recordType;
    }

    public ValueType getValueType() {
        return valueType;
    }

    @JsonSerialize(using = IntentSerializer.class)
    public Intent getIntent() {
        return intent;
    }

    public RejectionType getRejectionType() {
        return rejectionType;
    }

    public String getRejectionReason() {
        return rejectionReason;
    }

    public Long getRequestId() {
        return requestId;
    }

    public int getRequestStreamId() {
        return requestStreamId;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public String getBrokerVersion() {
        return brokerVersion;
    }

    public Integer getRecordVersion() {
        return recordVersion;
    }

    public String getAuthData() {
        return authData;
    }

    public JsonNode getRecordValue() {
        return recordValue;
    }

    @JsonIgnore
    public ProcessInstanceRelatedValue getPiRelatedValue() {
        return piRelatedValue;
    }

    // Kotlin data class componentN() backward compatibility
    public long component1() { return position; }
    public long component2() { return sourceRecordPosition; }
    public long component3() { return timestamp; }
    public long component4() { return key; }
    public RecordType component5() { return recordType; }
    public ValueType component6() { return valueType; }
    public Intent component7() { return intent; }
    public RejectionType component8() { return rejectionType; }
    public String component9() { return rejectionReason; }

    @Override
    public String toString() {
        try {
            var sw = new StringWriter();
            var gen = OBJECT_MAPPER.getFactory().createGenerator(sw);
            gen.writeStartObject();
            gen.writeNumberField("position", position);
            gen.writeNumberField("sourceRecordPosition", sourceRecordPosition);
            gen.writeNumberField("timestamp", timestamp);
            gen.writeNumberField("key", key);
            gen.writeStringField("recordType", recordType.name());
            gen.writeStringField("valueType", valueType.name());
            gen.writeStringField("intent", intent.name());
            // Match kotlinx-serialization: skip fields that equal their default values
            if (rejectionType != null && rejectionType != RejectionType.NULL_VAL) {
                gen.writeStringField("rejectionType", rejectionType.name());
            }
            if (rejectionReason != null && !rejectionReason.isEmpty()) {
                gen.writeStringField("rejectionReason", rejectionReason);
            }
            if (requestId != null && requestId != 0L) {
                gen.writeNumberField("requestId", requestId);
            }
            if (requestStreamId != 0) {
                gen.writeNumberField("requestStreamId", requestStreamId);
            }
            gen.writeNumberField("protocolVersion", protocolVersion);
            gen.writeStringField("brokerVersion", brokerVersion);
            if (recordVersion != null && recordVersion != 0) {
                gen.writeNumberField("recordVersion", recordVersion);
            }
            if (authData != null && !authData.isEmpty()) {
                gen.writeStringField("authData", authData);
            }
            gen.writeFieldName("recordValue");
            gen.writeTree(recordValue);
            gen.writeEndObject();
            gen.close();
            return sw.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Record to JSON", e);
        }
    }
}
