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
package io.zell.zdb.log

import io.zell.zdb.raft.RaftLogReader
import java.nio.file.Path


class LogStatus(logPath: Path) {

    private val reader: RaftLogReader = LogFactory.newReader(logPath)

    fun status(): LogStatusDetails {
        val logStatusDetails = LogStatusDetails()

        reader.forEachRemaining {
            val persistedRaftRecord = it.persistedRaftRecord

            if (logStatusDetails.highestTerm < persistedRaftRecord.term()) {
                logStatusDetails.highestTerm = persistedRaftRecord.term()
            }

            val currentEntryIndex = persistedRaftRecord.index()
            if (logStatusDetails.highestIndex < currentEntryIndex) {
                logStatusDetails.highestIndex = currentEntryIndex
            }

            if (logStatusDetails.lowestIndex > currentEntryIndex) {
                logStatusDetails.lowestIndex = currentEntryIndex
            }

            if (it.isApplicationEntry) {
                val applicationEntry = it.applicationEntry
                if (logStatusDetails.highestRecordPosition < applicationEntry.highestPosition()) {
                    logStatusDetails.highestRecordPosition = applicationEntry.highestPosition()
                }

                if (logStatusDetails.lowestRecordPosition > applicationEntry.lowestPosition()) {
                    logStatusDetails.lowestRecordPosition = applicationEntry.lowestPosition()
                }
            }
        }

        return logStatusDetails
    }


}
