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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JsonPrinterTest {

  private ByteArrayOutputStream out;
  private JsonPrinter jsonPrinter;

  @BeforeEach
  private void setup() {
    out = new ByteArrayOutputStream();
    jsonPrinter = new JsonPrinter(new PrintStream(out));
  }

  @Test
  public void shouldPrintEmptyDataObject() {
    // given

    // when
    jsonPrinter.surround((printer) -> {});

    // then
    Assertions.assertThat(out.toString()).isEqualTo("{\"data\":[]}");
  }

  @Test
  public void shouldPrintDataObjectWithOnElement() {
    // given

    // when
    jsonPrinter.surround(
        (printer) -> {
          printer.accept("{\"foo\":1}");
        });

    // then
    Assertions.assertThat(out.toString()).isEqualTo("{\"data\":[{\"foo\":1}]}");
  }

  @Test
  public void shouldPrintDataObjectWithMoreElements() {
    // given

    // when
    jsonPrinter.surround(
        (printer) -> {
          printer.accept("{\"foo\":1}");
          printer.accept("{\"foo\":1}");
          printer.accept("{\"foo\":1}");
        });

    // then
    Assertions.assertThat(out.toString())
        .isEqualTo("{\"data\":[{\"foo\":1},{\"foo\":1},{\"foo\":1}]}");
  }
}
