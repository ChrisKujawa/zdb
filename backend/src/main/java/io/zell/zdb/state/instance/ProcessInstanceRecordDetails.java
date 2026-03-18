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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.protocol.record.value.BpmnEventType;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ProcessInstanceRecordDetails {
    private final String bpmnProcessId;
    private final int version;
    private final String tenantId;
    private final long processDefinitionKey;
    private final long processInstanceKey;
    private final String elementId;
    private final long flowScopeKey;
    private final BpmnElementType bpmnElementType;
    private final BpmnEventType bpmnEventType;
    private final long parentProcessInstanceKey;
    private final long parentElementInstanceKey;

    @JsonCreator
    public ProcessInstanceRecordDetails(
            @JsonProperty("bpmnProcessId") String bpmnProcessId,
            @JsonProperty("version") int version,
            @JsonProperty("tenantId") String tenantId,
            @JsonProperty("processDefinitionKey") long processDefinitionKey,
            @JsonProperty("processInstanceKey") long processInstanceKey,
            @JsonProperty("elementId") String elementId,
            @JsonProperty("flowScopeKey") long flowScopeKey,
            @JsonProperty("bpmnElementType") BpmnElementType bpmnElementType,
            @JsonProperty("bpmnEventType") BpmnEventType bpmnEventType,
            @JsonProperty("parentProcessInstanceKey") long parentProcessInstanceKey,
            @JsonProperty("parentElementInstanceKey") long parentElementInstanceKey) {
        this.bpmnProcessId = bpmnProcessId;
        this.version = version;
        this.tenantId = tenantId;
        this.processDefinitionKey = processDefinitionKey;
        this.processInstanceKey = processInstanceKey;
        this.elementId = elementId;
        this.flowScopeKey = flowScopeKey;
        this.bpmnElementType = bpmnElementType;
        this.bpmnEventType = bpmnEventType;
        this.parentProcessInstanceKey = parentProcessInstanceKey;
        this.parentElementInstanceKey = parentElementInstanceKey;
    }

    public String getBpmnProcessId() {
        return bpmnProcessId;
    }

    public int getVersion() {
        return version;
    }

    public String getTenantId() {
        return tenantId;
    }

    public long getProcessDefinitionKey() {
        return processDefinitionKey;
    }

    public long getProcessInstanceKey() {
        return processInstanceKey;
    }

    public String getElementId() {
        return elementId;
    }

    public long getFlowScopeKey() {
        return flowScopeKey;
    }

    public BpmnElementType getBpmnElementType() {
        return bpmnElementType;
    }

    public BpmnEventType getBpmnEventType() {
        return bpmnEventType;
    }

    public long getParentProcessInstanceKey() {
        return parentProcessInstanceKey;
    }

    public long getParentElementInstanceKey() {
        return parentElementInstanceKey;
    }
}
