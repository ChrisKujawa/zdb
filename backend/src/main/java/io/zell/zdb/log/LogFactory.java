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

import io.zell.zdb.journal.file.SegmentedReadOnlyJournal;
import io.zell.zdb.raft.RaftLogReader;
import io.zell.zdb.raft.RaftLogUncommittedReader;
import java.nio.file.Path;

public class LogFactory {

    private static final String PARTITION_NAME_FORMAT = "raft-partition-partition-%d";
    private static final int MAX_SEGMENT_SIZE = 128 * 1024 * 1024;

    private LogFactory() {}

    public static RaftLogReader newReader(Path logPath) {
        var partitionName = extractPartitionNameFromPath(logPath);

        var builder = SegmentedReadOnlyJournal.builder();
        var readOnlyJournal = builder
                .withDirectory(logPath.toFile())
                .withName(partitionName)
                .withMaxSegmentSize(MAX_SEGMENT_SIZE)
                .build();

        return new RaftLogUncommittedReader(readOnlyJournal.openReader());
    }

    private static String extractPartitionNameFromPath(Path logPath) {
        try {
            var partitionId = Integer.parseInt(logPath.getFileName().toString());
            return String.format(PARTITION_NAME_FORMAT, partitionId);
        } catch (NumberFormatException nfe) {
            var errorMsg = String.format(
                    "Expected to extract partition as integer from path, but path was '%s'.",
                    logPath);
            throw new IllegalArgumentException(errorMsg, nfe);
        }
    }
}
