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

import io.zell.zdb.log.LogContentReader;
import io.zell.zdb.log.LogSearch;
import io.zell.zdb.log.LogStatus;
import io.zell.zdb.log.LogWriter;
import java.io.PrintStream;
import java.nio.file.Path;

public final class LogOutput {

  private LogOutput() {}

  public static String status(final Path logPath) {
    return new LogStatus(logPath).status().toString();
  }

  public static void writeStatus(final PrintStream out, final Path logPath) {
    out.println();
    out.println(status(logPath));
  }

  public static String printJson(
      final Path logPath, final long fromPosition, final long toPosition, final long instanceKey) {
    return OutputSupport.capture(
        out -> writeJson(out, logPath, fromPosition, toPosition, instanceKey));
  }

  public static String contentJson(final Path logPath) {
    return new LogContentReader(logPath).readAll().toString();
  }

  public static void writeJson(
      final PrintStream out,
      final Path logPath,
      final long fromPosition,
      final long toPosition,
      final long instanceKey) {
    final var logContentReader = filteredReader(logPath, fromPosition, toPosition, instanceKey);
    out.println("[");
    var separator = "";
    while (logContentReader.hasNext()) {
      final var record = logContentReader.next();

      out.print(separator + record);
      separator = ",";
    }
    out.println("]");
  }

  public static String printTable(
      final Path logPath, final long fromPosition, final long toPosition, final long instanceKey) {
    return OutputSupport.capture(
        out -> writeTable(out, logPath, fromPosition, toPosition, instanceKey));
  }

  public static void writeTable(
      final PrintStream out,
      final Path logPath,
      final long fromPosition,
      final long toPosition,
      final long instanceKey) {
    new LogWriter(out, filteredReader(logPath, fromPosition, toPosition, instanceKey))
        .writeAsTable();
  }

  public static String printDot(final Path logPath) {
    return new LogContentReader(logPath).readAll().asDotFile();
  }

  public static void writeDot(final PrintStream out, final Path logPath) {
    out.println(printDot(logPath));
  }

  public static String searchPosition(final Path logPath, final long position) {
    final var record = new LogSearch(logPath).searchPosition(position);
    return record == null ? "{}" : record.toString();
  }

  public static void writeSearchPosition(
      final PrintStream out, final Path logPath, final long position) {
    out.println(searchPosition(logPath, position));
  }

  public static String searchIndex(final Path logPath, final long index) {
    final var logContent = new LogSearch(logPath).searchIndex(index);
    return logContent == null ? "{}" : logContent.toString();
  }

  public static void writeSearchIndex(final PrintStream out, final Path logPath, final long index) {
    out.println(searchIndex(logPath, index));
  }

  private static LogContentReader filteredReader(
      final Path logPath, final long fromPosition, final long toPosition, final long instanceKey) {
    final var logContentReader = new LogContentReader(logPath);
    logContentReader.seekToPosition(fromPosition);
    logContentReader.limitToPosition(toPosition);
    if (instanceKey > 0) {
      logContentReader.filterForProcessInstance(instanceKey);
    }
    return logContentReader;
  }
}
