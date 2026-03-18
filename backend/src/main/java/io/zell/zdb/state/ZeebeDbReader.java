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
package io.zell.zdb.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.db.impl.ZeebeDbConstants;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.protocol.impl.encoding.MsgPackConverter;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.*;

public class ZeebeDbReader {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final RocksDB rocksDb;

    public ZeebeDbReader(Path statePath) {
        var options = new Options(
                new DBOptions(),
                new ColumnFamilyOptions()
                        .setSstPartitionerFactory(new SstPartitionerFixedPrefixFactory(Long.BYTES))
                        .useFixedLengthPrefixExtractor(Long.BYTES));

        try {
            this.rocksDb = OptimisticTransactionDB.openReadOnly(
                    options,
                    statePath.toString());
        } catch (RocksDBException rocksEx) {
            if (rocksEx.getStatus().getCode() == Status.Code.IOError) {
                var message = rocksEx.getMessage();
                if (message != null && message.contains("No such file or directory")) {
                    throw sneakyThrow(new FileNotFoundException(
                            "Expected to find RocksDB instance under given path " + statePath
                                    + ", but nothing found."));
                }
            }
            throw sneakyThrow(rocksEx);
        }
    }

    @FunctionalInterface
    public interface Visitor {
        void visit(String cf, byte[] key, byte[] value);
    }

    @FunctionalInterface
    public interface JsonValueVisitor {
        void visit(String cf, byte[] key, String valueJson);
    }

    @FunctionalInterface
    public interface JsonValueWithKeyPrefixVisitor {
        void visit(byte[] key, String valueJson);
    }

    private byte[] convertColumnFamilyToArray(ZbColumnFamilies cf) {
        var array = new byte[Long.BYTES];
        var buffer = new UnsafeBuffer(array);
        buffer.putLong(0, cf.ordinal(), ZeebeDbConstants.ZB_DB_BYTE_ORDER);
        return array;
    }

    public void visitDBWithPrefix(ZbColumnFamilies cf, JsonValueWithKeyPrefixVisitor visitor) {
        var prefixArray = convertColumnFamilyToArray(cf);
        var readOptions = new ReadOptions()
                .setPrefixSameAsStart(true)
                .setTotalOrderSeek(false);
        try (var iterator = rocksDb.newIterator(rocksDb.getDefaultColumnFamily(), readOptions)) {
            iterator.seek(prefixArray);
            while (iterator.isValid()) {
                var key = iterator.key();
                var value = iterator.value();
                var unsafeBuffer = new UnsafeBuffer(key);
                var enumValue = unsafeBuffer.getLong(0, ZeebeDbConstants.ZB_DB_BYTE_ORDER);
                var kvCF = ZbColumnFamilies.values()[(int) enumValue];

                if (cf == kvCF) {
                    var jsonValue = MsgPackConverter.convertToJson(value);
                    visitor.visit(key, jsonValue);
                }
                iterator.next();
            }
        }
    }

    public void visitDB(Visitor visitor) {
        try (var iterator = rocksDb.newIterator(rocksDb.getDefaultColumnFamily(), new ReadOptions())) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                var key = iterator.key();
                var value = iterator.value();
                var unsafeBuffer = new UnsafeBuffer(key);
                var enumValue = unsafeBuffer.getLong(0, ZeebeDbConstants.ZB_DB_BYTE_ORDER);
                String cfName;

                var enumOrdinal = (int) enumValue;
                if (enumOrdinal < ZbColumnFamilies.values().length) {
                    cfName = ZbColumnFamilies.values()[enumOrdinal].name();
                } else {
                    cfName = enumOrdinal + " (UNKNOWN)";
                }

                visitor.visit(cfName, key, value);
                iterator.next();
            }
        }
    }

    public void visitDBWithJsonValues(JsonValueVisitor visitor) {
        visitDB((cf, key, value) -> {
            var jsonValue = MsgPackConverter.convertToJson(value);
            visitor.visit(cf, key, jsonValue);
        });
    }

    public byte[] getValue(ZbColumnFamilies cf, long key) {
        var keyArray = new byte[Long.BYTES + Long.BYTES];
        var buffer = new UnsafeBuffer(keyArray);

        buffer.putLong(0, cf.ordinal(), ZeebeDbConstants.ZB_DB_BYTE_ORDER);
        buffer.putLong(Long.BYTES, key, ZeebeDbConstants.ZB_DB_BYTE_ORDER);

        try {
            return rocksDb.get(keyArray);
        } catch (RocksDBException e) {
            throw sneakyThrow(e);
        }
    }

    public String getValueAsJson(ZbColumnFamilies cf, long key) {
        var bytes = getValue(cf, key);
        if (bytes != null) {
            return MsgPackConverter.convertToJson(bytes);
        }
        return "{}";
    }

    public Map<String, Integer> stateStatistics() {
        var countMap = new LinkedHashMap<String, Integer>();

        visitDB((cf, key, value) -> countMap.merge(cf, 1, Integer::sum));
        return countMap;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> RuntimeException sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }

    public String stateStatisticsAsJsonString() {
        var stateStatistics = stateStatistics();
        try {
            return OBJECT_MAPPER.writeValueAsString(stateStatistics);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize state statistics to JSON", e);
        }
    }
}
