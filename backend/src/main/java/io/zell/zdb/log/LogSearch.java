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

import io.zell.zdb.log.records.ApplicationRecord;
import io.zell.zdb.log.records.PersistedRecord;
import io.zell.zdb.log.records.Record;
import java.nio.file.Path;

public class LogSearch {

    private final LogContentReader reader;

    public LogSearch(Path logPath) {
        this.reader = new LogContentReader(logPath);
    }

    public Record searchPosition(long position) {
        if (position <= 0) {
            return null;
        }

        reader.seekToPosition(position);

        while (reader.hasNext()) {
            var entry = reader.next();

            if (entry instanceof ApplicationRecord applicationRecord) {
                if (applicationRecord.getLowestPosition() > position) {
                    return null;
                } else if (applicationRecord.getHighestPosition() < position) {
                    continue;
                } else {
                    for (var record : applicationRecord.getEntries()) {
                        if (record.position() == position) {
                            return record;
                        }
                    }
                }
            }
        }
        return null;
    }

    public PersistedRecord searchIndex(long index) {
        if (index <= 0) {
            return null;
        }

        reader.seekToIndex(index);

        while (reader.hasNext()) {
            var entry = reader.next();

            if (entry.index() == index) {
                return entry;
            }
        }

        return null;
    }
}
