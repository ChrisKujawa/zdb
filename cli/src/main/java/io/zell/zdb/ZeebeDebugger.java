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

import io.zell.zdb.ZeebeDebugger.VersionProvider;
import io.zell.zdb.journal.LogCommand;
import io.zell.zdb.journal.RaftCommand;
import io.zell.zdb.state.InstanceCommand;
import io.zell.zdb.state.ProcessCommand;
import io.zell.zdb.state.StateCommand;
import java.util.concurrent.Callable;
import picocli.AutoComplete.GenerateCompletion;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.RunLast;

@Command(
    name = "zdb",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    description = "Zeebe debug and inspection tool",
    subcommands = {
      GenerateCompletion.class, // to generate auto completion
      ProcessCommand.class,
      InstanceCommand.class,
      IncidentCommand.class,
      LogCommand.class,
      RaftCommand.class,
      StateCommand.class
    })
public class ZeebeDebugger implements Callable<Integer> {
  private static CommandLine cli;

  /**
   * Disables the error stream to prevent IllegalAccess warnings to be logged.
   * https://stackoverflow.com/questions/46454995/how-to-hide-warning-illegal-reflective-access-in-java-9-without-jvm-argument
   */
  public static void disableWarning() {

    System.err.close();
    System.setErr(System.out);
  }

  public static void main(String[] args) {
    disableWarning();
    cli =
        new CommandLine(new ZeebeDebugger())
            .setExecutionStrategy(new RunLast())
            .setCaseInsensitiveEnumValuesAllowed(true);
    final int exitcode = cli.execute(args);
    System.exit(exitcode);
  }

  @Override
  public Integer call() {
    cli.usage(System.out);
    return 0;
  }

  static class VersionProvider implements IVersionProvider {
    public String[] getVersion() {
      return new String[] {"zdb v" + ZeebeDebugger.class.getPackage().getImplementationVersion()};
    }
  }
}
