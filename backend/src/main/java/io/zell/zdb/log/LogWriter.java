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

import io.zell.zdb.log.records.PersistedRecord;
import java.io.OutputStream;
import java.io.PrintWriter;

public class LogWriter {

    public static final String COLUMN_TITLE =
            "Index Term Position SourceRecordPosition Timestamp Key RecordType ValueType Intent ProcessInstanceKey BPMNElementType ";

    private final OutputStream out;
    private final LogContentReader reader;

    public LogWriter(OutputStream out, LogContentReader reader) {
        this.out = out;
        this.reader = reader;
    }

    public void writeAsTable() {
        var printWriter = new PrintWriter(out, true);
        printWriter.println(COLUMN_TITLE);
        var separator = "";
        while (reader.hasNext()) {
            PersistedRecord record = reader.next();
            printWriter.print(separator + record.asColumnString());
            separator = "";
        }
        printWriter.flush();
    }
}
