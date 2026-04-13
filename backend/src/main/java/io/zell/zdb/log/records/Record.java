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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.Intent;
import java.io.StringWriter;

public record Record(
    long position,
    long sourceRecordPosition,
    long timestamp,
    long key,
    RecordType recordType,
    ValueType valueType,
    @JsonSerialize(using = IntentSerializer.class) Intent intent,
    RejectionType rejectionType,
    String rejectionReason,
    Long requestId,
    int requestStreamId,
    int protocolVersion,
    String brokerVersion,
    Integer recordVersion,
    String authData,
    JsonNode recordValue,
    @JsonIgnore ProcessInstanceRelatedValue piRelatedValue) {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
