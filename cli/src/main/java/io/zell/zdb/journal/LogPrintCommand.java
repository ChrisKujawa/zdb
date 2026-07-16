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
package io.zell.zdb.journal;

import io.zell.zdb.output.LogOutput;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(name = "print", description = "Print's the complete log to standard out")
public class LogPrintCommand implements Callable<Integer> {

  public enum Format {
    JSON,
    DOT,
    TABLE,
  }

  @Spec private CommandSpec spec;

  @Option(
      names = {"-f", "--format"},
      description =
          "Print's the complete log in the specified format, defaults to json. Possible values: [ ${COMPLETION-CANDIDATES} ]",
      defaultValue = "JSON")
  private Format format;

  @Option(
      names = {"--from", "--fromPosition"},
      description =
          "Option to skip the begin of log and only print from the given position."
              + " Note this is on best effort basis, since engine records are written in batches."
              + " There might be some more records printed before the given position (part of the written batch).",
      defaultValue = "0")
  private long fromPosition;

  @Option(
      names = {"--to", "--toPosition"},
      description =
          "Option to set a limit to print the log only to the given position."
              + " Note this is on best effort basis, since engine records are written in batches."
              + " There might be some more records printed after the given position (part of the written batch).",
      defaultValue = Long.MAX_VALUE + "")
  private long toPosition;

  @Option(
      names = {"--instanceKey"},
      description =
          "Filter to print only records which are part the specified process instance."
              + " Note this is on best effort basis, since engine records are written in batches."
              + " There might be some records printed which do not have an process instance key assigned."
              + " RaftRecords are completely skipped, if this filter is applied.",
      defaultValue = "0")
  private long instanceKey;

  @Override
  public Integer call() {
    final Path partitionPath = spec.findOption("-p").getValue();

    switch (format) {
      case DOT -> LogOutput.writeDot(System.out, partitionPath);
      case TABLE ->
          LogOutput.writeTable(System.out, partitionPath, fromPosition, toPosition, instanceKey);
      default ->
          LogOutput.writeJson(System.out, partitionPath, fromPosition, toPosition, instanceKey);
    }
    return 0;
  }
}
