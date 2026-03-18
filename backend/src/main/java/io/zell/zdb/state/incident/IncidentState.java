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
package io.zell.zdb.state.incident;

import io.camunda.zeebe.db.impl.ZeebeDbConstants;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.zell.zdb.state.JsonElementVisitor;
import io.zell.zdb.state.ZeebeDbReader;
import java.nio.file.Path;
import org.agrona.concurrent.UnsafeBuffer;

public class IncidentState {

    private final ZeebeDbReader zeebeDbReader;

    public IncidentState(Path statePath) {
        this.zeebeDbReader = new ZeebeDbReader(statePath);
    }

    public void listIncidents(JsonElementVisitor visitor) {
        zeebeDbReader.visitDBWithPrefix(
                ZbColumnFamilies.INCIDENTS,
                (key, valueJson) -> {
                    var incidentKey = Long.toString(
                            new UnsafeBuffer(key).getLong(Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER));
                    visitor.visit("{\"key\": " + incidentKey + ", \"value\": " + valueJson + "}");
                });
    }

    public String incidentDetails(long incidentKey) {
        return zeebeDbReader.getValueAsJson(ZbColumnFamilies.INCIDENTS, incidentKey);
    }
}
