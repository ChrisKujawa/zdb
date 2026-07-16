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

import java.io.PrintStream;
import java.util.function.Consumer;

/** Utility class used by commands and tests to print valid {@code {"data":[...]}} JSON. */
public class JsonPrinter {

  private boolean moreThanOneElement = false;

  private final PrintStream stream;

  public JsonPrinter() {
    this(System.out);
  }

  public JsonPrinter(final PrintStream stream) {
    this.stream = stream;
  }

  private void printHeading() {
    stream.print("{\"data\":[");
  }

  private void printSeparator() {
    stream.print(",");
  }

  public void printElement(final String element) {
    if (moreThanOneElement) {
      printSeparator();
    }
    stream.print(element);
    moreThanOneElement = true;
  }

  private void printEnd() {
    stream.print("]}");
  }

  public void surround(final Consumer<Consumer<String>> toBeSurrounded) {
    printHeading();
    toBeSurrounded.accept(this::printElement);
    printEnd();
  }

  public void reset() {
    moreThanOneElement = false;
  }
}
