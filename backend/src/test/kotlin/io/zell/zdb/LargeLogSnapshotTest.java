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

import static org.assertj.core.api.Assertions.assertThat;

import io.zell.zdb.log.LogStatus;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class LargeLogSnapshotTest {

  private static final Path SNAPSHOT_ZIP =
      Path.of("src/test/resources/zeebe-states/large-log-v8.8.zip");
  private static final String SNAPSHOT_NAME = "large-log-v8.8";
  private static final String PARTITION = "1";

  private static SnapshotFixture fixture;
  private static Path snapshotDir;

  @BeforeAll
  static void setUp() throws Exception {
    fixture = SnapshotFixture.unzip(SNAPSHOT_ZIP, SNAPSHOT_NAME);
    snapshotDir = fixture.snapshotDir();
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (fixture != null) {
      fixture.cleanup();
    }
  }

  @Test
  void shouldReadLargeLogStatus() throws IOException {
    // given
    final var logStatus = new LogStatus(logPath());

    // when
    final var status = logStatus.status();

    // then
    assertThat(status.getHighestIndex()).isGreaterThan(200);
    assertThat(status.getHighestRecordPosition()).isGreaterThan(250);
    assertThat(status.getLowestIndex()).isEqualTo(1);
    assertThat(status.getLowestRecordPosition()).isEqualTo(1);
    try (final var files = Files.list(logPath())) {
      assertThat(files.filter(path -> path.getFileName().toString().endsWith(".log")).count())
          .as("large-log fixture should span multiple log segments")
          .isGreaterThan(1);
    }
  }

  private static Path logPath() {
    return ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
  }
}
