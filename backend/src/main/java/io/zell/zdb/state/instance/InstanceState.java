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
package io.zell.zdb.state.instance;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.zell.zdb.state.ZeebeDbReader;
import java.nio.file.Path;
import java.util.function.Predicate;

public class InstanceState {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final ZeebeDbReader zeebeDbReader;

    public InstanceState(Path statePath) {
        this.zeebeDbReader = new ZeebeDbReader(statePath);
    }

    public String getInstance(long elementInstanceKey) {
        return zeebeDbReader.getValueAsJson(ZbColumnFamilies.ELEMENT_INSTANCE_KEY, elementInstanceKey);
    }

    public void listInstances(ZeebeDbReader.JsonValueWithKeyPrefixVisitor visitor) {
        zeebeDbReader.visitDBWithPrefix(ZbColumnFamilies.ELEMENT_INSTANCE_KEY, visitor);
    }

    public void listProcessInstances(Predicate<ProcessInstanceRecordDetails> predicate,
                                     ZeebeDbReader.JsonValueWithKeyPrefixVisitor visitor) {
        zeebeDbReader.visitDBWithPrefix(ZbColumnFamilies.ELEMENT_INSTANCE_KEY,
                (key, value) -> {
                    try {
                        var instanceDetails = objectMapper.readValue(value, InstanceDetails.class);
                        var processInstanceRecord = instanceDetails.getElementRecord().getProcessInstanceRecord();
                        if (processInstanceRecord.getBpmnElementType() == BpmnElementType.PROCESS
                                && predicate.test(processInstanceRecord)) {
                            visitor.visit(key, value);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to deserialize InstanceDetails", e);
                    }
                });
    }
}
