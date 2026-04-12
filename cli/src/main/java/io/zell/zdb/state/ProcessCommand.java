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

import io.zell.zdb.JsonPrinter;
import io.zell.zdb.state.instance.InstanceState;
import io.zell.zdb.state.process.ProcessState;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ScopeType;
import picocli.CommandLine.Spec;

@Command(
    name = "process",
    mixinStandardHelpOptions = true,
    description = "Print's information about deployed processes")
public class ProcessCommand implements Callable<Integer> {

  @Spec private CommandSpec spec;

  @Option(
      names = {"-p", "--path"},
      paramLabel = "PARTITION_PATH",
      description = "The path to the partition data (either runtime or snapshot in partition dir)",
      required = true,
      scope = ScopeType.INHERIT)
  private Path partitionPath;

  @Override
  public Integer call() {
    spec.commandLine().usage(System.out);
    return 0;
  }

  @Command(name = "list", description = "List all processes")
  public int list() {
    new JsonPrinter()
        .surround(
            (printer) ->
                new ProcessState(partitionPath)
                    .listProcesses((key, valueJson) -> printer.accept(valueJson)));
    return 0;
  }

  @Command(name = "entity", description = "Show details about a process")
  public int entity(
      @Parameters(paramLabel = "KEY", description = "The key of the process", arity = "1")
          final long key) {
    new JsonPrinter()
        .surround(
            (printer) ->
                new ProcessState(partitionPath)
                    .processDetails(key, (k, valueJson) -> printer.accept(valueJson)));
    return 0;
  }

  @Command(name = "instances", description = "Show all instances of a process")
  public int instances(
      @Parameters(paramLabel = "KEY", description = "The key of the process", arity = "1")
          final long key) {

    new JsonPrinter()
        .surround(
            (printer) ->
                new InstanceState(partitionPath)
                    .listProcessInstances(
                        processInstanceRecordDetails ->
                            processInstanceRecordDetails.processDefinitionKey() == key,
                        (key1, valueJson) -> printer.accept(valueJson)));

    return 0;
  }
}
