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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ApplicationRecord implements PersistedRecord {
    private final long index;
    private final long term;
    private final long highestPosition;
    private final long lowestPosition;
    private final List<Record> entries = new ArrayList<>();

    public ApplicationRecord(long index, long term, long highestPosition, long lowestPosition) {
        this.index = index;
        this.term = term;
        this.highestPosition = highestPosition;
        this.lowestPosition = lowestPosition;
    }

    @Override
    public long index() {
        return index;
    }

    public long getIndex() {
        return index;
    }

    @Override
    public long term() {
        return term;
    }

    public long getTerm() {
        return term;
    }

    public long getHighestPosition() {
        return highestPosition;
    }

    public long getLowestPosition() {
        return lowestPosition;
    }

    public List<Record> getEntries() {
        return entries;
    }

    public String entriesAsJson() {
        return entries.stream()
                .map(Record::toString)
                .collect(Collectors.joining(","));
    }

    @Override
    public String toString() {
        return "{\"index\":" + index
                + ",\"term\":" + term
                + ",\"highestPosition\":" + highestPosition
                + ",\"lowestPosition\":" + lowestPosition
                + ",\"entries\":[" + entriesAsJson() + "]}";
    }

    public String entryAsColumn(Record record) {
        var sb = new StringBuilder();
        var separator = " ";
        sb.append(record.position())
                .append(separator)
                .append(record.sourceRecordPosition())
                .append(separator)
                .append(record.timestamp())
                .append(separator)
                .append(record.key())
                .append(separator)
                .append(record.recordType())
                .append(separator)
                .append(record.valueType())
                .append(separator)
                .append(record.intent());

        var piRelatedValue = record.piRelatedValue();
        if (piRelatedValue != null) {
            if (piRelatedValue.processInstanceKey() != null) {
                sb.append(separator)
                        .append(piRelatedValue.processInstanceKey())
                        .append(separator);
            }

            if (piRelatedValue.bpmnElementType() != null) {
                sb.append(piRelatedValue.bpmnElementType())
                        .append(separator);
            }
        }
        return sb.toString();
    }

    @Override
    public String asColumnString() {
        var prefix = index + " " + term + " ";
        var sb = new StringBuilder();
        for (var entry : entries) {
            sb.append(prefix).append(entryAsColumn(entry)).append(System.lineSeparator());
        }
        return sb.toString();
    }
}
