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

@JsonIgnoreProperties(ignoreUnknown = true)
public class InstanceDetails {
    private final long parentKey;
    private final int multiInstanceLoopCounter;
    private final int childTerminatedCount;
    private final int childCompletedCount;
    private final int childActivatedCount;
    private final int childCount;
    private final String interruptingEventKeyProp;
    private final Long calledChildInstanceKeyProp;
    private final ElementDetails elementRecord;

    @JsonCreator
    public InstanceDetails(
            @JsonProperty("parentKey") long parentKey,
            @JsonProperty("multiInstanceLoopCounter") int multiInstanceLoopCounter,
            @JsonProperty("childTerminatedCount") int childTerminatedCount,
            @JsonProperty("childCompletedCount") int childCompletedCount,
            @JsonProperty("childActivatedCount") int childActivatedCount,
            @JsonProperty("childCount") int childCount,
            @JsonProperty("interruptingEventKeyProp") String interruptingEventKeyProp,
            @JsonProperty("calledChildInstanceKeyProp") Long calledChildInstanceKeyProp,
            @JsonProperty("elementRecord") ElementDetails elementRecord) {
        this.parentKey = parentKey;
        this.multiInstanceLoopCounter = multiInstanceLoopCounter;
        this.childTerminatedCount = childTerminatedCount;
        this.childCompletedCount = childCompletedCount;
        this.childActivatedCount = childActivatedCount;
        this.childCount = childCount;
        this.interruptingEventKeyProp = interruptingEventKeyProp;
        this.calledChildInstanceKeyProp = calledChildInstanceKeyProp;
        this.elementRecord = elementRecord;
    }

    public long getParentKey() {
        return parentKey;
    }

    public int getMultiInstanceLoopCounter() {
        return multiInstanceLoopCounter;
    }

    public int getChildTerminatedCount() {
        return childTerminatedCount;
    }

    public int getChildCompletedCount() {
        return childCompletedCount;
    }

    public int getChildActivatedCount() {
        return childActivatedCount;
    }

    public int getChildCount() {
        return childCount;
    }

    public String getInterruptingEventKeyProp() {
        return interruptingEventKeyProp;
    }

    public Long getCalledChildInstanceKeyProp() {
        return calledChildInstanceKeyProp;
    }

    public ElementDetails getElementRecord() {
        return elementRecord;
    }
}
