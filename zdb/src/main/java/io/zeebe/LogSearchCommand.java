/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
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
package io.zeebe;

import io.zeebe.impl.log.LogSearch;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(
    name = "search",
    mixinStandardHelpOptions = true,
    description = "Search's in the log for a given position or index")
public class LogSearchCommand implements Callable<Integer> {

  @Spec private CommandSpec spec;

  @ArgGroup(exclusive = true, multiplicity = "1")
  private Exclusive exclusive;

  @Override
  public Integer call() {
    final Path logPath = spec.findOption("-p").getValue();
    final String result;
    if (exclusive.index == 0) {
      result = new LogSearch().searchForPosition(logPath, exclusive.position);
    } else {
      result = new LogSearch().searchForIndex(logPath, exclusive.index);
    }
    System.out.println(result);
    return 1;
  }

  static class Exclusive {
    @Option(
        names = {"-pos", "--position"},
        paramLabel = "POSITION",
        description = "The position of a record to search for.",
        required = true)
    private long position;

    @Option(
        names = {"-idx", "--index"},
        paramLabel = "INDEX",
        description = "The index of an entry to search for.",
        required = true)
    private long index;
  }
}
