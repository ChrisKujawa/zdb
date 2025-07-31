package io.zell.zdb.raft

import io.atomix.raft.storage.serializer.MetaStoreSerializer
import io.atomix.raft.storage.system.Configuration
import io.atomix.raft.storage.system.MetaStoreRecord
import java.io.IOException
import java.io.UncheckedIOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

class MetaStoreReader @JvmOverloads constructor(
    private val metaPath: Path,
    private val configPath: Path,
    private val serializer: MetaStoreSerializer = MetaStoreSerializer()
) {
    fun readConfig(): Configuration {
        try {
            val bytes = Files.readAllBytes(configPath)
            val buffer = ByteBuffer.wrap(bytes)
            return serializer.readConfiguration(buffer)
        } catch (e: IOException) {
            throw UncheckedIOException("Failed to read configuration from $configPath", e)
        }
    }

    fun readMetaStore(): MetaStoreRecord {
        try {
            FileChannel.open(metaPath, StandardOpenOption.READ).use { channel ->
                var count: Int
                do {
                    count = channel.read(serializer.metaByteBuffer())
                } while (count > 0)
            }
        } catch (e: IOException) {
            throw UncheckedIOException("Failed to read meta store record from $metaPath", e)
        }

        return serializer.readRecord()
    }
}