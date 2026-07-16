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

import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.zell.zdb.JsonPrinter;
import io.zell.zdb.state.KeyFormatters;
import io.zell.zdb.state.ZeebeDbReader;
import java.io.PrintStream;
import java.nio.file.Path;

public final class StateOutput {

  public static final String ENTRY_FORMAT = "\n{\"cf\":\"%s\",\"key\":\"%s\",\"value\":%s}";

  private StateOutput() {}

  public static String statistics(final Path partitionPath) {
    return new ZeebeDbReader(partitionPath).stateStatisticsAsJsonString();
  }

  public static void writeStatistics(final PrintStream out, final Path partitionPath) {
    out.println(statistics(partitionPath));
  }

  public static String list(
      final Path partitionPath, final String columnFamilyName, final String keyFormat) {
    return OutputSupport.capture(out -> writeList(out, partitionPath, columnFamilyName, keyFormat));
  }

  public static void writeList(
      final PrintStream out,
      final Path partitionPath,
      final String columnFamilyName,
      final String keyFormat) {
    final var keyFormatters = chooseKeyFormatters(keyFormat);

    new JsonPrinter(out)
        .surround(
            (printer) -> {
              final var zeebeDbReader = new ZeebeDbReader(partitionPath);
              if (noColumnFamilyGiven(columnFamilyName)) {
                zeebeDbReader.visitDBWithJsonValues(
                    ((cfName, key, valueJson) -> {
                      final var cf = ZbColumnFamilies.valueOf(cfName);
                      printer.accept(
                          String.format(
                              ENTRY_FORMAT,
                              cf,
                              keyFormatters.forColumnFamily(cf).formatKey(key),
                              valueJson));
                    }));
              } else {
                final var cf = ZbColumnFamilies.valueOf(columnFamilyName.toUpperCase());
                zeebeDbReader.visitDBWithPrefix(
                    cf,
                    ((key, valueJson) ->
                        printer.accept(
                            String.format(
                                ENTRY_FORMAT,
                                cf,
                                keyFormatters.forColumnFamily(cf).formatKey(key),
                                valueJson))));
              }
            });
  }

  private static KeyFormatters chooseKeyFormatters(final String keyFormat) {
    if (keyFormat == null || keyFormat.isEmpty() || "default".equals(keyFormat)) {
      return KeyFormatters.ofDefault();
    } else if ("hex".equals(keyFormat)) {
      return KeyFormatters.ofHex();
    }
    return KeyFormatters.ofFormat(keyFormat);
  }

  private static boolean noColumnFamilyGiven(final String columnFamilyName) {
    return columnFamilyName == null || columnFamilyName.isEmpty();
  }
}
