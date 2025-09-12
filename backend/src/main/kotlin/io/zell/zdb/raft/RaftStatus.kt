package io.zell.zdb.raft

import io.atomix.raft.cluster.RaftMember
import io.atomix.raft.storage.system.Configuration
import io.atomix.raft.storage.system.MetaStoreRecord
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.nio.file.Path

class RaftStatus(partitionPath: Path) {
    private val json = Json { ignoreUnknownKeys = true }
    private val reader: MetaStoreReader

    init {
        val partitionId = partitionPath.fileName.toString()
        val metaPath = partitionPath.resolve("raft-partition-partition-$partitionId.meta")
        val configPath =
            partitionPath.resolve("raft-partition-partition-$partitionId.conf")

        reader = MetaStoreReader(metaPath, configPath)
    }

    fun details(): RaftStatusDetails {
        return RaftStatusDetails(reader.readMetaStore(), reader.readConfig())
    }

    fun detailsAsJson(): String {
        return json.encodeToString(details());
    }

    @JvmRecord
    @Serializable
    data class RaftStatusDetails(val meta: MetaStoreDetails, val config: RaftConfigDetails) {
        constructor(metaStoreRecord: MetaStoreRecord, configuration: Configuration) : this(
            MetaStoreDetails(metaStoreRecord),
            RaftConfigDetails(configuration)
        )
    }

    @JvmRecord
    @Serializable
    data class MetaStoreDetails(
        val term: Long,
        val lastFlushedIndex: Long,
        val commitIndex: Long,
        val votedFor: String
    ) {
        constructor(metaStoreRecord: MetaStoreRecord) : this(
            metaStoreRecord.term,
            metaStoreRecord.lastFlushedIndex,
            metaStoreRecord.commitIndex,
            metaStoreRecord.votedFor ?: ""
        )
    }

    @JvmRecord
    @Serializable
    data class RaftConfigDetails(
        val index: Long,
        val term: Long,
        val time: Long,
        val force: Boolean,
        val requiresJointConsensus: Boolean,
        val newMembers: Collection<RaftMemberDetails>,
        val oldMembers: Collection<RaftMemberDetails>
    ) {
        constructor(configuration: Configuration) : this(
            configuration.index,
            configuration.term,
            configuration.time,
            configuration.force,
            configuration.requiresJointConsensus(),
            configuration.newMembers.map { RaftMemberDetails(it) },
            configuration.oldMembers.map { RaftMemberDetails(it) }
        )
    }

    @JvmRecord
    @Serializable
    data class RaftMemberDetails(
        val id: String,
        val hash: Int,
        val type: String,
        val lastUpdated: String
    ) {
        constructor(member: RaftMember) : this(
            member.memberId().id(),
            member.hash(),
            member.type.name,
            member.lastUpdated.toString()
        )
    }
}