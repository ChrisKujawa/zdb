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

import io.camunda.zeebe.protocol.record.ValueType;
import io.zell.zdb.log.records.ApplicationRecord;
import io.zell.zdb.log.records.PersistedRecord;
import io.zell.zdb.log.records.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogContent {
    private final List<PersistedRecord> records = new ArrayList<>();

    public List<PersistedRecord> getRecords() {
        return records;
    }

    @Override
    public String toString() {
        var recordsJson = records.stream()
                .map(PersistedRecord::toString)
                .collect(Collectors.joining(","));
        return "{ \"records\": [" + recordsJson + "] } ";
    }

    public String asDotFile() {
        var content = new StringBuilder("digraph log {")
                .append(System.lineSeparator())
                .append("rankdir=\"RL\"")
                .append(";")
                .append(System.lineSeparator());

        records.stream()
                .filter(r -> r instanceof ApplicationRecord)
                .map(r -> (ApplicationRecord) r)
                .flatMap(r -> r.getEntries().stream())
                .forEach(entry -> addEventAsDotNode(entry, content));

        content.append(System.lineSeparator())
                .append("}");
        return content.toString();
    }

    private void addEventAsDotNode(Record entry, StringBuilder content) {
        content.append(entry.getPosition())
                .append(" [label=\"")
                .append("\\n").append(entry.getRecordType())
                .append("\\n").append(entry.getValueType().name())
                .append("\\n").append(entry.getIntent().name());

        if (entry.getValueType() == ValueType.PROCESS_INSTANCE) {
            var piRelatedValue = entry.getPiRelatedValue();
            if (piRelatedValue != null) {
                if (piRelatedValue.bpmnElementType() != null) {
                    content.append("\\n").append(piRelatedValue.bpmnElementType());
                }

                if (piRelatedValue.processInstanceKey() != null) {
                    content.append("\\nPI Key: ").append(piRelatedValue.processInstanceKey());
                }

                if (piRelatedValue.processDefinitionKey() != null) {
                    content.append("\\nPD Key: ").append(piRelatedValue.processDefinitionKey());
                }
            }
        }

        content
                .append("\\nKey: ").append(entry.getKey())
                .append("\"]")
                .append(";")
                .append(System.lineSeparator());
        if (entry.getSourceRecordPosition() != -1L) {
            content.append(entry.getPosition())
                    .append(" -> ")
                    .append(entry.getSourceRecordPosition())
                    .append(";")
                    .append(System.lineSeparator());
        }
    }
}
