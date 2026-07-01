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
package io.zell.zdb.v88;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zell.zdb.SnapshotMetadata;
import io.zell.zdb.ZeebePaths;
import io.zell.zdb.log.LogContentReader;
import io.zell.zdb.log.LogStatus;
import io.zell.zdb.log.LogWriter;
import io.zell.zdb.raft.RaftStatus;
import io.zell.zdb.state.ZeebeDbReader;
import io.zell.zdb.state.incident.IncidentState;
import io.zell.zdb.state.instance.InstanceState;
import io.zell.zdb.state.process.ProcessState;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class Version88GoldenTest {

  // Paths relative to backend/ module root (Maven CWD during test execution)
  static final Path SNAPSHOT_ZIP = Path.of("src/test/resources/zeebe-states/v8.8.zip");
  static final Path GOLDEN = Path.of("src/test/resources/golden/v8.8");
  static final boolean UPDATE = Boolean.getBoolean("updateGoldens");
  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Set in @BeforeAll after unzipping; points to the zeebe-states/v8.8 dir inside temp
  static Path snapshotDir;
  static SnapshotMetadata metadata;

  @BeforeAll
  static void setUp() throws Exception {
    final Path tempRoot = Files.createTempDirectory("zdb-v88-snapshot");
    try (var zis = new ZipInputStream(Files.newInputStream(SNAPSHOT_ZIP))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        final var dest = tempRoot.resolve(entry.getName());
        if (entry.isDirectory()) {
          Files.createDirectories(dest);
        } else {
          Files.createDirectories(dest.getParent());
          Files.copy(zis, dest);
        }
        zis.closeEntry();
      }
    }
    // Zip entries are prefixed with zeebe-states/v8.8/ so the data lives there
    snapshotDir = tempRoot.resolve("zeebe-states/v8.8");
    metadata =
        OBJECT_MAPPER.readValue(snapshotDir.resolve("metadata.json").toFile(), SnapshotMetadata.class);
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (snapshotDir != null) {
      // Walk the temp root (two levels up from snapshotDir = zeebe-states/v8.8)
      final Path tempRoot = snapshotDir.getParent().getParent();
      Files.walk(tempRoot)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    }
  }

  // ── State tests ──────────────────────────────────────────────────────────────

  @Test
  void shouldMatchStateStatistics() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");

    // when
    final var output = new ZeebeDbReader(runtimePath).stateStatisticsAsJsonString();

    // then
    assertOrUpdate("state-statistics.json", prettyPrint(output));
  }

  @Test
  void shouldMatchProcessList() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var items = new ArrayList<JsonNode>();

    // when
    new ProcessState(runtimePath).listProcesses((key, valueJson) -> parseAndAdd(items, valueJson));
    final var output = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(items);

    // then
    assertOrUpdate("process-list.json", output);
  }

  @Test
  void shouldMatchProcessDetails() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var processKey = metadata.firstProcessKey();
    final var items = new ArrayList<JsonNode>();

    // when
    new ProcessState(runtimePath)
        .processDetails(processKey, (key, valueJson) -> parseAndAdd(items, valueJson));
    assertThat(items).as("processDetails returned no results for key %d", processKey).isNotEmpty();
    final var output =
        OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(items.get(0));

    // then
    assertOrUpdate("process-entity.json", output);
  }

  @Test
  void shouldMatchInstanceDetails() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var processInstanceKey = metadata.processInstanceKey();

    // when
    final var output =
        prettyPrint(new InstanceState(runtimePath).getInstance(processInstanceKey));

    // then
    assertOrUpdate("instance-entity.json", output);
  }

  @Test
  void shouldMatchInstanceList() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var items = new ArrayList<JsonNode>();

    // when
    new InstanceState(runtimePath).listInstances((key, valueJson) -> parseAndAdd(items, valueJson));
    final var output = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(items);

    // then
    assertOrUpdate("instance-list.json", output);
  }

  @Test
  void shouldMatchIncidentList() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var items = new ArrayList<JsonNode>();

    // when
    new IncidentState(runtimePath).listIncidents(json -> parseAndAdd(items, json));
    final var output = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(items);

    // then
    assertOrUpdate("incident-list.json", output);
  }

  @Test
  void shouldMatchIncidentDetails() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var incidentKey = metadata.incidentKey();

    // when
    final var output =
        prettyPrint(new IncidentState(runtimePath).incidentDetails(incidentKey));

    // then
    assertOrUpdate("incident-entity.json", output);
  }

  // ── Log tests ────────────────────────────────────────────────────────────────

  @Test
  void shouldMatchLogStatus() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var output = prettyPrint(new LogStatus(logPath).status().toString());

    // then
    assertOrUpdate("log-status.json", output);
  }

  @Test
  void shouldMatchRaftStatus() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var output = prettyPrint(new RaftStatus(logPath).detailsAsJson());

    // then
    assertOrUpdate("raft-status.json", output);
  }

  @Test
  void shouldMatchLogContentJson() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var content = new LogContentReader(logPath).readAll();
    final var output = prettyPrint(content.toString());

    // then
    assertOrUpdate("log-print.json", output);
  }

  @Test
  void shouldMatchLogContentTable() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");
    final var baos = new ByteArrayOutputStream();

    // when
    final var reader = new LogContentReader(logPath);
    new LogWriter(baos, reader).writeAsTable();
    final var output = baos.toString(StandardCharsets.UTF_8);

    // then
    assertOrUpdate("log-print.table", output);
  }

  @Test
  void shouldMatchLogContentDot() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var content = new LogContentReader(logPath).readAll();
    final var output = content.asDotFile();

    // then
    assertOrUpdate("log-print.dot", output);
  }

  // ── Helpers ──────────────────────────────────────────────────────────────────

  private void assertOrUpdate(final String filename, final String actual) throws IOException {
    final var goldenFile = GOLDEN.resolve(filename);
    if (UPDATE) {
      Files.createDirectories(goldenFile.getParent());
      Files.writeString(goldenFile, actual);
    } else {
      final var expected = Files.readString(goldenFile);
      assertThat(actual).isEqualTo(expected);
    }
  }

  private String prettyPrint(final String json) throws JsonProcessingException {
    return OBJECT_MAPPER
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(OBJECT_MAPPER.readTree(json));
  }

  private void parseAndAdd(final ArrayList<JsonNode> items, final String json) {
    try {
      items.add(OBJECT_MAPPER.readTree(json));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
