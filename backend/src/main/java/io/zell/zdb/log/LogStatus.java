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

import io.zell.zdb.raft.RaftLogReader;
import java.nio.file.Path;

public class LogStatus {

    private final RaftLogReader reader;

    public LogStatus(Path logPath) {
        this.reader = LogFactory.newReader(logPath);
    }

    public LogStatusDetails status() {
        var logStatusDetails = new LogStatusDetails();

        reader.forEachRemaining(it -> {
            var persistedRaftRecord = it.getPersistedRaftRecord();

            if (logStatusDetails.getHighestTerm() < persistedRaftRecord.term()) {
                logStatusDetails.setHighestTerm(persistedRaftRecord.term());
            }

            var currentEntryIndex = persistedRaftRecord.index();
            if (logStatusDetails.getHighestIndex() < currentEntryIndex) {
                logStatusDetails.setHighestIndex(currentEntryIndex);
            }

            if (logStatusDetails.getLowestIndex() > currentEntryIndex) {
                logStatusDetails.setLowestIndex(currentEntryIndex);
            }

            if (it.isApplicationEntry()) {
                var applicationEntry = it.getApplicationEntry();
                if (logStatusDetails.getHighestRecordPosition() < applicationEntry.highestPosition()) {
                    logStatusDetails.setHighestRecordPosition(applicationEntry.highestPosition());
                }

                if (logStatusDetails.getLowestRecordPosition() > applicationEntry.lowestPosition()) {
                    logStatusDetails.setLowestRecordPosition(applicationEntry.lowestPosition());
                }
            }
        });

        return logStatusDetails;
    }
}
