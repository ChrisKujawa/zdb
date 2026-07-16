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

import io.zell.zdb.output.StateOutput;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "state",
    mixinStandardHelpOptions = true,
    description = "Prints general information of the internal state")
public class StateCommand implements Callable<Integer> {

  @Option(
      names = {"-p", "--path"},
      paramLabel = "PARTITION_PATH",
      description = "The path to the partition data (either runtime or snapshot in partition dir)",
      scope = CommandLine.ScopeType.INHERIT,
      required = true)
  private Path partitionPath;

  @Override
  public Integer call() {
    StateOutput.writeStatistics(System.out, partitionPath);
    return 0;
  }

  @Command(name = "list", description = "List column families and the values as json")
  public int list(
      @Option(
              names = {"-cf", "--columnFamily"},
              paramLabel = "COLUMNFAMILY",
              description = "The column family name to filter for")
          final String columnFamilyName,
      @Option(
              names = {"-kf", "--keyFormat"},
              paramLabel = "KEY_FORMAT",
              description =
                  "The format of the key (default, hex, or a format string like 'silbB' for 'string, int, long, byte, byte[])")
          final String keyFormat) {
    StateOutput.writeList(System.out, partitionPath, columnFamilyName, keyFormat);
    return 0;
  }
}
