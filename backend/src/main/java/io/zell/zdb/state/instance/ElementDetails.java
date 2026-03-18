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
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ElementDetails {
    private final long key;
    private final ProcessInstanceIntent state;
    private final ProcessInstanceRecordDetails processInstanceRecord;

    @JsonCreator
    public ElementDetails(
            @JsonProperty("key") long key,
            @JsonProperty("state") ProcessInstanceIntent state,
            @JsonProperty("processInstanceRecord") ProcessInstanceRecordDetails processInstanceRecord) {
        this.key = key;
        this.state = state;
        this.processInstanceRecord = processInstanceRecord;
    }

    public long getKey() {
        return key;
    }

    public ProcessInstanceIntent getState() {
        return state;
    }

    public ProcessInstanceRecordDetails getProcessInstanceRecord() {
        return processInstanceRecord;
    }
}
