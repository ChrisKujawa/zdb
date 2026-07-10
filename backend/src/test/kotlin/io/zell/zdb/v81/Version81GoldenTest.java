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
package io.zell.zdb.v81;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.zell.zdb.GoldenFileAssert;
import io.zell.zdb.SnapshotFixture;
import io.zell.zdb.SnapshotMetadata;
import io.zell.zdb.ZeebePaths;
import io.zell.zdb.log.LogContentReader;
import io.zell.zdb.log.LogSearch;
import io.zell.zdb.log.LogStatus;
import io.zell.zdb.log.LogWriter;
import io.zell.zdb.raft.RaftStatus;
import io.zell.zdb.state.KeyFormatters;
import io.zell.zdb.state.ZeebeDbReader;
import io.zell.zdb.state.incident.IncidentState;
import io.zell.zdb.state.instance.InstanceState;
import io.zell.zdb.state.process.ProcessState;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class Version81GoldenTest {

  // Paths relative to backend/ module root (Maven CWD during test execution)
  static final Path SNAPSHOT_ZIP = Path.of("src/test/resources/zeebe-states/v8.1.zip");
  static final Path GOLDEN = Path.of("src/test/resources/golden/v8.1");
  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Set in @BeforeAll after unzipping; points to the zeebe-states/v8.1 dir inside temp
  static SnapshotFixture fixture;
  static Path snapshotDir;
  static SnapshotMetadata metadata;

  @BeforeAll
  public static void setUp() throws Exception {
    fixture = SnapshotFixture.unzip(SNAPSHOT_ZIP, "v8.1");
    snapshotDir = fixture.snapshotDir();
    metadata = fixture.metadata();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (fixture != null) {
      fixture.cleanup();
    }
  }

  // ── State tests ──────────────────────────────────────────────────────────────

  @Test
  public void shouldMatchStateStatistics() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");

    // when
    final var output = new ZeebeDbReader(runtimePath).stateStatisticsAsJsonString();

    // then
    assertOrUpdate("state-statistics.json", prettyPrint(output));
  }

  @Test
  public void shouldMatchStateList() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var keyFormatters = KeyFormatters.ofDefault();
    final var sb = new StringBuilder();
    sb.append("{\"data\":[");
    final boolean[] first = {true};

    // when
    new ZeebeDbReader(runtimePath)
        .visitDBWithJsonValues(
            (cfName, key, valueJson) -> {
              if (!first[0]) sb.append(",");
              first[0] = false;
              final var cf = ZbColumnFamilies.valueOf(cfName);
              sb.append(
                  String.format(
                      "\n{\"cf\":\"%s\",\"key\":\"%s\",\"value\":%s}",
                      cf, keyFormatters.forColumnFamily(cf).formatKey(key), valueJson));
            });
    sb.append("]}");

    // then
    assertOrUpdate("state-list.json", sb.toString());
  }

  @Test
  public void shouldMatchProcessList() throws IOException {
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
  public void shouldMatchProcessDetails() throws IOException {
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
  public void shouldMatchProcessInstances() throws IOException {
    // given
    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), "1");
    final var processKey = metadata.firstProcessKey();
    final var items = new ArrayList<JsonNode>();

    // when
    new InstanceState(runtimePath)
        .listProcessInstances(
            r -> r.getProcessDefinitionKey() == processKey,
            (key, valueJson) -> parseAndAdd(items, valueJson));
    final var output = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(items);

    // then
    assertOrUpdate("process-instances.json", output);
  }

  @Test
  public void shouldMatchInstanceDetails() throws IOException {
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
  public void shouldMatchInstanceList() throws IOException {
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
  public void shouldMatchIncidentList() throws IOException {
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
  public void shouldMatchIncidentDetails() throws IOException {
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
  public void shouldMatchLogStatus() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var output = prettyPrint(new LogStatus(logPath).status().toString());

    // then
    assertOrUpdate("log-status.json", output);
  }

  @Test
  public void shouldMatchRaftStatus() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var output = prettyPrint(new RaftStatus(logPath).detailsAsJson());

    // then
    assertOrUpdate("raft-status.json", output);
  }

  @Test
  public void shouldMatchLogContentJson() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var content = new LogContentReader(logPath).readAll();
    final var output = prettyPrint(content.toString());

    // then
    assertOrUpdate("log-print.json", output);
  }

  @Test
  public void shouldMatchLogContentTable() throws IOException {
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
  public void shouldMatchLogContentDot() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when
    final var content = new LogContentReader(logPath).readAll();
    final var output = content.asDotFile();

    // then
    assertOrUpdate("log-print.dot", output);
  }

  @Test
  public void shouldMatchLogSearchByPosition() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when — position 1 is the lowest record position; always present in the snapshot
    final var record = new LogSearch(logPath).searchPosition(1);
    assertThat(record).isNotNull();
    final var output = prettyPrint(record.toString());

    // then
    assertOrUpdate("log-search-position.json", output);
  }

  @Test
  public void shouldMatchLogSearchByIndex() throws IOException {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), "1");

    // when — index 1 is the lowest Raft index; always present in the snapshot
    final var record = new LogSearch(logPath).searchIndex(1);
    assertThat(record).isNotNull();
    final var output = prettyPrint(record.toString());

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

  private void parseAndAdd(final ArrayList<JsonNode> items, final String json) {
    try {
      items.add(OBJECT_MAPPER.readTree(json));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
