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
import io.zell.zdb.state.instance.InstanceState;
import java.io.PrintStream;
import java.nio.file.Path;

public final class InstanceOutput {

  private InstanceOutput() {}

  public static String entity(final Path partitionPath, final long key) {
    return new InstanceState(partitionPath).getInstance(key);
  }

  public static void writeEntity(final PrintStream out, final Path partitionPath, final long key) {
    out.println(entity(partitionPath, key));
  }

  public static String list(final Path partitionPath) {
    return OutputSupport.capture(out -> writeList(out, partitionPath));
  }

  public static void writeList(final PrintStream out, final Path partitionPath) {
    new JsonPrinter(out)
        .surround(
            printer ->
                new InstanceState(partitionPath)
                    .listInstances((key, valueJson) -> printer.accept(valueJson)));
  }
}
