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
package io.zell.zdb;

import io.zell.zdb.output.IncidentOutput;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine.*;
import picocli.CommandLine.Model.CommandSpec;

@Command(
    name = "incident",
    mixinStandardHelpOptions = true,
    description = "Print's information about created incident's")
public class IncidentCommand implements Callable<Integer> {

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

  @Command(name = "list", description = "List all incidents")
  public int list() {
    IncidentOutput.writeList(System.out, partitionPath);
    return 0;
  }

  @Command(name = "entity", description = "Show details about an incident")
  public int entity(
      @Parameters(paramLabel = "KEY", description = "The key of the incident", arity = "1")
          final long key) {
    IncidentOutput.writeEntity(System.out, partitionPath, key);
    return 0;
  }
}
