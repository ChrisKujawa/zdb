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
package io.zell.zdb.mcp;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.DeploymentEvent;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import io.zeebe.containers.ZeebeContainer;
import io.zell.zdb.mcp.tools.IncidentTools;
import io.zell.zdb.mcp.tools.PartitionTools;
import io.zell.zdb.mcp.tools.ProcessInstanceTools;
import io.zell.zdb.mcp.tools.ProcessTools;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class ZdbMcpToolsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZdbMcpToolsTest.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final DockerImageName DOCKER_IMAGE =
      DockerImageName.parse("camunda/camunda:8.8.0");

  private static final String CONTAINER_DATA_PATH = "/usr/local/camunda/data/";

  private static final BpmnModelInstance PROCESS =
      Bpmn.createExecutableProcess("mcp-process")
          .startEvent()
          .serviceTask("incidentTask")
          .zeebeJobType("incidentTask")
          .zeebeJobRetriesExpression("=foo") // causes incident
          .endEvent()
          .done();

  private static final File TEMP_DIR = newTmpFolder();

  static {
    if (!TEMP_DIR.mkdirs() && !TEMP_DIR.isDirectory()) {
      throw new IllegalStateException("Could not create temp dir " + TEMP_DIR);
    }
  }

  /** A standalone Zeebe container that produces partition data into TEMP_DIR. */
  @Container
  public static ZeebeContainer zeebeContainer =
      new ZeebeContainer(DOCKER_IMAGE)
          .withCreateContainerCmdModifier(cmd -> cmd.withUser(getRunAsUser()))
          .withEnv("ZEEBE_BROKER_EXPERIMENTAL_ROCKSDB_DISABLEWAL", "false")
          .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "none")
          .withEnv("SPRING_PROFILES_ACTIVE", "broker,standalone")
          .withLogConsumer(new Slf4jLogConsumer(LOGGER))
          .withFileSystemBind(TEMP_DIR.getPath(), CONTAINER_DATA_PATH, BindMode.READ_WRITE);

  private static Path copiedDataPath;

  @BeforeAll
  public static void setup() throws Exception {
    createWorkload(zeebeContainer.getExternalGatewayAddress());

    // Copy data into a temp dir so we can read it after the container stops touching files.
    copiedDataPath = Files.createTempDirectory("zdb-mcp-test-");
    copyDirectory(TEMP_DIR.toPath(), copiedDataPath);
  }

  @AfterAll
  public static void cleanup() throws IOException {
    deleteRecursively(TEMP_DIR.toPath());
    if (copiedDataPath != null) {
      deleteRecursively(copiedDataPath);
    }
  }

  @Test
  public void shouldListPartitions() throws Exception {
    // when
    final CallToolResult result =
        PartitionTools.listPartitionsCall(copiedDataPath.toAbsolutePath().toString());

    // then
    assertThat(result.isError()).isFalse();
    final JsonNode json = parseTextContent(result);
    assertThat(json.get("exists").asBoolean()).isTrue();
    final JsonNode partitions = json.get("partitions");
    assertThat(partitions.isArray()).isTrue();
    assertThat(partitions.size()).isGreaterThanOrEqualTo(1);
    final JsonNode first = partitions.get(0);
    assertThat(first.get("id").asInt()).isEqualTo(1);
    assertThat(first.get("partition_path").asText()).contains("partitions");
  }

  @Test
  public void shouldGetPartitionStatus() throws Exception {
    // given
    final String partitionPath = firstPartitionPath();

    // when
    final CallToolResult result = PartitionTools.getPartitionStatusCall(partitionPath);

    // then
    assertThat(result.isError()).isFalse();
    final JsonNode json = parseTextContent(result);
    assertThat(json.has("log")).isTrue();
    assertThat(json.has("raft")).isTrue();
    final JsonNode log = json.get("log");
    assertThat(log.has("highestIndex")).isTrue();
    assertThat(log.has("highestTerm")).isTrue();
    final JsonNode raft = json.get("raft");
    assertThat(raft.has("meta")).isTrue();
    assertThat(raft.has("config")).isTrue();
  }

  @Test
  public void shouldListProcessInstances() throws Exception {
    // given
    final String partitionPath = firstPartitionPath();

    // when
    final CallToolResult result =
        ProcessInstanceTools.listProcessInstancesCall(partitionPath, null, null, 0, 50);

    // then
    assertThat(result.isError()).isFalse();
    final JsonNode json = parseTextContent(result);
    assertThat(json.has("data")).isTrue();
    assertThat(json.get("data").isArray()).isTrue();
    assertThat(json.get("total_fetched").asInt()).isGreaterThanOrEqualTo(1);
  }

  @Test
  public void shouldListIncidents() throws Exception {
    // given
    final String partitionPath = firstPartitionPath();

    // when
    final CallToolResult result =
        IncidentTools.listIncidentsCall(partitionPath, null, null, null, 0, 50);

    // then
    assertThat(result.isError()).isFalse();
    final JsonNode json = parseTextContent(result);
    assertThat(json.has("data")).isTrue();
    assertThat(json.get("data").isArray()).isTrue();
    // We expect at least one incident from the workload (job retries expression).
    assertThat(json.get("total_fetched").asInt()).isGreaterThanOrEqualTo(1);
  }

  @Test
  public void shouldListProcesses() throws Exception {
    // given
    final String partitionPath = firstPartitionPath();

    // when
    final CallToolResult result = ProcessTools.listProcessesCall(partitionPath, 0, 50);

    // then
    assertThat(result.isError()).isFalse();
    final JsonNode json = parseTextContent(result);
    assertThat(json.has("data")).isTrue();
    assertThat(json.get("total_fetched").asInt()).isGreaterThanOrEqualTo(1);
  }

  @Test
  public void shouldReturnErrorForInvalidPath() {
    final CallToolResult result =
        PartitionTools.getPartitionStatusCall("/non/existent/path-zdb-mcp");
    assertThat(result.isError()).isTrue();
    assertThat(textOf(result)).startsWith("Error:");
  }

  // -- helpers ---------------------------------------------------------------------------

  private static String firstPartitionPath() throws Exception {
    final CallToolResult result =
        PartitionTools.listPartitionsCall(copiedDataPath.toAbsolutePath().toString());
    final JsonNode json = parseTextContent(result);
    return json.get("partitions").get(0).get("partition_path").asText();
  }

  private static JsonNode parseTextContent(final CallToolResult result) throws IOException {
    return OBJECT_MAPPER.readTree(textOf(result));
  }

  private static String textOf(final CallToolResult result) {
    final TextContent text = (TextContent) result.content().get(0);
    return text.text();
  }

  private static void createWorkload(final String gatewayAddress) {
    try (ZeebeClient client =
        ZeebeClient.newClientBuilder().gatewayAddress(gatewayAddress).usePlaintext().build()) {

      final DeploymentEvent deployment =
          client
              .newDeployResourceCommand()
              .addProcessModel(PROCESS, "mcp-process.bpmn")
              .send()
              .join();
      assertThat(deployment.getProcesses()).hasSize(1);

      client
          .newCreateInstanceCommand()
          .bpmnProcessId("mcp-process")
          .latestVersion()
          .variables(Map.of("foo", "not-a-number"))
          .send()
          .join();

      // Allow the broker time to materialise the job + create the incident.
      try {
        Thread.sleep(2000L);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static void copyDirectory(final Path source, final Path target) throws IOException {
    try (Stream<Path> walk = Files.walk(source)) {
      walk.forEach(
          path -> {
            try {
              final Path dst = target.resolve(source.relativize(path).toString());
              if (Files.isDirectory(path)) {
                Files.createDirectories(dst);
              } else {
                Files.createDirectories(dst.getParent());
                Files.copy(path, dst, StandardCopyOption.REPLACE_EXISTING);
              }
            } catch (final IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }
  }

  private static void deleteRecursively(final Path path) throws IOException {
    if (!Files.exists(path)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(path)) {
      walk.sorted((a, b) -> b.getNameCount() - a.getNameCount())
          .forEach(
              p -> {
                try {
                  Files.deleteIfExists(p);
                } catch (final IOException ignored) {
                  // best-effort cleanup
                }
              });
    }
  }

  private static File newTmpFolder() {
    return new File(
        "/tmp/",
        "zdb-mcp-test-"
            + ZdbMcpToolsTest.class.getName()
            + "-"
            + ThreadLocalRandom.current().nextLong());
  }

  private static String getRunAsUser() {
    return execCommand("id -u") + ":" + execCommand("id -g");
  }

  private static String execCommand(final String command) {
    try {
      final Process process = Runtime.getRuntime().exec(command);
      try (var reader =
          new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()))) {
        return reader.readLine();
      }
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
