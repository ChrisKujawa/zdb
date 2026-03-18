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
package io.zell.zdb.state.process;

import io.camunda.zeebe.db.impl.ZeebeDbConstants;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.zell.zdb.state.ZeebeDbReader;
import java.nio.file.Path;
import org.agrona.concurrent.UnsafeBuffer;

public class ProcessState {

    private final ZeebeDbReader zeebeDbReader;

    public ProcessState(Path statePath) {
        this.zeebeDbReader = new ZeebeDbReader(statePath);
    }

    public void listProcesses(ZeebeDbReader.JsonValueWithKeyPrefixVisitor visitor) {
        zeebeDbReader.visitDBWithPrefix(ZbColumnFamilies.DEPRECATED_PROCESS_CACHE, visitor);
        // for 8.3
        zeebeDbReader.visitDBWithPrefix(ZbColumnFamilies.PROCESS_CACHE, visitor);
    }

    public void processDetails(long processDefinitionKey, ZeebeDbReader.JsonValueWithKeyPrefixVisitor visitor) {
        var found = new boolean[]{false};

        zeebeDbReader.visitDBWithPrefix(ZbColumnFamilies.DEPRECATED_PROCESS_CACHE, (key, value) -> {
            var keyBuffer = new UnsafeBuffer(key);
            var currentProcessDefinitionKey = keyBuffer.getLong(
                    keyBuffer.capacity() - Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER);

            if (currentProcessDefinitionKey == processDefinitionKey) {
                found[0] = true;
                visitor.visit(key, value);
            }
        });

        if (!found[0]) {
            // for 8.3
            zeebeDbReader.visitDBWithPrefix(ZbColumnFamilies.PROCESS_CACHE, (key, value) -> {
                var keyBuffer = new UnsafeBuffer(key);
                var currentProcessDefinitionKey = keyBuffer.getLong(
                        keyBuffer.capacity() - Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER);

                if (currentProcessDefinitionKey == processDefinitionKey) {
                    visitor.visit(key, value);
                }
            });
        }
    }
}
