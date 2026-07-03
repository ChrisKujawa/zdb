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

import com.github.difflib.DiffUtils;
import com.github.difflib.UnifiedDiffUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.opentest4j.AssertionFailedError;

/**
 * Compares rendered ZDB output against a committed golden file, or (re)writes it when {@code
 * -DupdateGoldens=true} is set. On mismatch, throws with a unified diff in the message so CI/console
 * logs point straight at the changed lines, while still passing the full expected/actual strings to
 * {@link AssertionFailedError} so IDEs can render their own side-by-side diff view.
 */
public final class GoldenFileAssert {

  private static final boolean UPDATE = Boolean.getBoolean("updateGoldens");

  private GoldenFileAssert() {}

  public static void assertOrUpdate(final Path goldenFile, final String actual) throws IOException {
    if (UPDATE) {
      Files.createDirectories(goldenFile.getParent());
      Files.writeString(goldenFile, actual);
      return;
    }

    final String expected = Files.readString(goldenFile);
    if (actual.equals(expected)) {
      return;
    }

    throw new AssertionFailedError(
        "Golden file mismatch: "
            + goldenFile
            + System.lineSeparator()
            + unifiedDiff(goldenFile, expected, actual),
        expected,
        actual);
  }

  private static String unifiedDiff(final Path goldenFile, final String expected, final String actual) {
    final List<String> expectedLines = expected.lines().toList();
    final List<String> actualLines = actual.lines().toList();
    final var patch = DiffUtils.diff(expectedLines, actualLines);
    final List<String> diffLines =
        UnifiedDiffUtils.generateUnifiedDiff(
            goldenFile + " (expected)", goldenFile + " (actual)", expectedLines, patch, 3);
    return String.join(System.lineSeparator(), diffLines);
  }
}
