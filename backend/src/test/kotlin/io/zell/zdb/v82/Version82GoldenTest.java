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
package io.zell.zdb.v82;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zell.zdb.GoldenFileAssert;
import io.zell.zdb.SnapshotFixture;
import io.zell.zdb.SnapshotMetadata;
import io.zell.zdb.ZeebePaths;
import io.zell.zdb.output.IncidentOutput;
import io.zell.zdb.output.InstanceOutput;
import io.zell.zdb.output.LogOutput;
import io.zell.zdb.output.ProcessOutput;
import io.zell.zdb.output.RaftOutput;
import io.zell.zdb.output.StateOutput;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class Version82GoldenTest {

  // Paths relative to backend/ module root (Maven CWD during test execution)
  static final Path SNAPSHOT_ZIP = Path.of("src/test/resources/zeebe-states/v8.2.zip");
  static final Path GOLDEN = Path.of("src/test/resources/golden/v8.2");
  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Set in @BeforeAll after unzipping; points to the zeebe-states/v8.2 dir inside temp
  static SnapshotFixture fixture;
  static Path snapshotDir;
  static SnapshotMetadata metadata;

  @BeforeAll
  static void setUp() throws Exception {
    fixture = SnapshotFixture.unzip(SNAPSHOT_ZIP, "v8.2");
    snapshotDir = fixture.snapshotDir();
    metadata = fixture.metadata();
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (fixture != null) {
      fixture.cleanup();
    }
  }

  // ── State tests ──────────────────────────────────────────────────────────────

  @Test
  void shouldMatchStateStatistics() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");

    // when
    final var output = StateOutput.statistics(runtimePath);

    // then
    assertOrUpdate("state-statistics.json", prettyPrint(output));
  }

  @Test
  void shouldMatchStateList() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");

    // when
    final var output = StateOutput.list(runtimePath, "", "default");

    // then
    assertOrUpdate("state-list.json", output);
  }

  @Test
  void shouldMatchProcessList() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");

    // when
    final var output = prettyPrint(ProcessOutput.list(runtimePath));

    // then
    assertOrUpdate("process-list.json", output);
  }

  @Test
  void shouldMatchProcessDetails() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var processKey = metadata.firstProcessKey();

    // when
    final var output = prettyPrint(ProcessOutput.entity(runtimePath, processKey));

    // then
    assertOrUpdate("process-entity.json", output);
  }

  @Test
  void shouldMatchProcessInstances() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var processKey = metadata.firstProcessKey();

    // when
    final var output = prettyPrint(ProcessOutput.instances(runtimePath, processKey));

    // then
    assertOrUpdate("process-instances.json", output);
  }

  @Test
  void shouldMatchInstanceDetails() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var processInstanceKey = metadata.processInstanceKey();

    // when
    final var output = prettyPrint(InstanceOutput.entity(runtimePath, processInstanceKey));

    // then
    assertOrUpdate("instance-entity.json", output);
  }

  @Test
  void shouldMatchInstanceList() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");

    // when
    final var output = prettyPrint(InstanceOutput.list(runtimePath));

    // then
    assertOrUpdate("instance-list.json", output);
  }

  @Test
  void shouldMatchIncidentList() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");

    // when
    final var output = prettyPrint(IncidentOutput.list(runtimePath));

    // then
    assertOrUpdate("incident-list.json", output);
  }

  @Test
  void shouldMatchIncidentDetails() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var incidentKey = metadata.incidentKey();

    // when
    final var output = prettyPrint(IncidentOutput.entity(runtimePath, incidentKey));

    // then
    assertOrUpdate("incident-entity.json", output);
  }

  // ── Log tests ────────────────────────────────────────────────────────────────

  @Test
  void shouldMatchLogStatus() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var output = prettyPrint(LogOutput.status(logPath));

    // then
    assertOrUpdate("log-status.json", output);
  }

  @Test
  void shouldMatchRaftStatus() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var output = prettyPrint(RaftOutput.json(logPath));

    // then
    assertOrUpdate("raft-status.json", output);
  }

  @Test
  void shouldMatchLogContentJson() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var output = prettyPrint(LogOutput.contentJson(logPath));

    // then
    assertOrUpdate("log-print.json", output);
  }

  @Test
  void shouldMatchLogContentTable() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var output = LogOutput.printTable(logPath, 0, Long.MAX_VALUE, 0);

    // then
    assertOrUpdate("log-print.table", output);
  }

  @Test
  void shouldMatchLogContentDot() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var output = LogOutput.printDot(logPath);

    // then
    assertOrUpdate("log-print.dot", output);
  }

  @Test
  void shouldMatchLogSearchByPosition() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when — position 1 is the lowest record position; always present in the snapshot
    final var output = prettyPrint(LogOutput.searchPosition(logPath, 1));

    // then
    assertOrUpdate("log-search-position.json", output);
  }

  @Test
  void shouldMatchLogSearchByIndex() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when — index 1 is the lowest Raft index; always present in the snapshot
    final var output = prettyPrint(LogOutput.searchIndex(logPath, 1));

    // then
    assertOrUpdate("log-search-index.json", output);
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private void assertOrUpdate(final String filename, final String actual) throws IOException {
    GoldenFileAssert.assertOrUpdate(GOLDEN.resolve(filename), actual);
  }

  private String prettyPrint(final String json) throws JsonProcessingException {
    return OBJECT_MAPPER
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(OBJECT_MAPPER.readTree(json));
  }
}
