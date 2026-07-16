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
package io.zell.zdb.output;

import io.zell.zdb.JsonPrinter;
import io.zell.zdb.state.incident.IncidentState;
import java.io.PrintStream;
import java.nio.file.Path;

public final class IncidentOutput {

  private IncidentOutput() {}

  public static String list(final Path partitionPath) {
    return OutputSupport.jsonArray(
        item -> new IncidentState(partitionPath).listIncidents(item::accept));
  }

  public static void writeList(final PrintStream out, final Path partitionPath) {
    new JsonPrinter(out)
        .surround(
            (printer) -> {
              final var incidentState = new IncidentState(partitionPath);
              incidentState.listIncidents(printer::accept);
            });
  }

  public static String entity(final Path partitionPath, final long key) {
    return new IncidentState(partitionPath).incidentDetails(key);
  }

  public static void writeEntity(final PrintStream out, final Path partitionPath, final long key) {
    out.println(entity(partitionPath, key));
  }
}
