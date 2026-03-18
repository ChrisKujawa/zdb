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
package io.zell.zdb.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.atomix.raft.cluster.RaftMember;
import io.atomix.raft.storage.system.Configuration;
import io.atomix.raft.storage.system.MetaStoreRecord;
import java.nio.file.Path;
import java.util.Collection;

public class RaftStatus {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final MetaStoreReader reader;

    public RaftStatus(Path partitionPath) {
        var partitionId = partitionPath.getFileName().toString();
        var metaPath = partitionPath.resolve("raft-partition-partition-" + partitionId + ".meta");
        var configPath = partitionPath.resolve("raft-partition-partition-" + partitionId + ".conf");

        this.reader = new MetaStoreReader(metaPath, configPath);
    }

    public RaftStatusDetails details() {
        return new RaftStatusDetails(reader.readMetaStore(), reader.readConfig());
    }

    public String detailsAsJson() {
        try {
            return OBJECT_MAPPER.writeValueAsString(details());
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize RaftStatusDetails to JSON", e);
        }
    }

    public record RaftStatusDetails(MetaStoreDetails meta, RaftConfigDetails config) {
        public RaftStatusDetails(MetaStoreRecord metaStoreRecord, Configuration configuration) {
            this(new MetaStoreDetails(metaStoreRecord), new RaftConfigDetails(configuration));
        }
    }

    public record MetaStoreDetails(long term, long lastFlushedIndex, long commitIndex, String votedFor) {
        public MetaStoreDetails(MetaStoreRecord metaStoreRecord) {
            this(
                    metaStoreRecord.term(),
                    metaStoreRecord.lastFlushedIndex(),
                    metaStoreRecord.commitIndex(),
                    metaStoreRecord.votedFor() != null ? metaStoreRecord.votedFor() : "");
        }
    }

    public record RaftConfigDetails(
            long index,
            long term,
            long time,
            boolean force,
            boolean requiresJointConsensus,
            Collection<RaftMemberDetails> newMembers,
            Collection<RaftMemberDetails> oldMembers) {
        public RaftConfigDetails(Configuration configuration) {
            this(
                    configuration.index(),
                    configuration.term(),
                    configuration.time(),
                    configuration.force(),
                    configuration.requiresJointConsensus(),
                    configuration.newMembers().stream().map(RaftMemberDetails::new).toList(),
                    configuration.oldMembers().stream().map(RaftMemberDetails::new).toList());
        }
    }

    public record RaftMemberDetails(String id, int hash, String type, String lastUpdated) {
        public RaftMemberDetails(RaftMember member) {
            this(
                    member.memberId().id(),
                    member.hash(),
                    member.getType().name(),
                    member.getLastUpdated().toString());
        }
    }
}
