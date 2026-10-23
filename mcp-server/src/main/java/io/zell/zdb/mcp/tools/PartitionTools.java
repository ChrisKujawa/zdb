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
package io.zell.zdb.mcp.tools;

import static io.zell.zdb.mcp.util.ToolUtils.MAPPER;
import static io.zell.zdb.mcp.util.ToolUtils.error;
import static io.zell.zdb.mcp.util.ToolUtils.ok;
import static io.zell.zdb.mcp.util.ToolUtils.requiredString;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.JsonSchema;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.zell.zdb.log.LogStatus;
import io.zell.zdb.raft.RaftStatus;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/** MCP tool specifications for discovering and inspecting partition directories. */
public final class PartitionTools {

  private PartitionTools() {}

  /**
   * @return all partition-discovery and partition-status tool specs.
   */
  public static List<McpServerFeatures.SyncToolSpecification> specs() {
    return List.of(listPartitions(), getPartitionStatus());
  }

  private static McpServerFeatures.SyncToolSpecification listPartitions() {
    final JsonSchema schema =
        new JsonSchema(
            "object",
            Map.of(
                "data_path",
                Map.of(
                    "type", "string",
                    "description",
                        "The root Zeebe data directory (the parent of raft-partition).")),
            List.of("data_path"),
            false,
            null,
            null);

    final Tool tool =
        Tool.builder()
            .name("list_partitions")
            .description(
                "Discovers Zeebe partition directories under the given data path. Returns "
                    + "partition IDs and their full paths. Always call this first before "
                    + "using other tools — use the returned partition_path values for all "
                    + "subsequent calls. Typical Zeebe data structure: "
                    + "<data_path>/raft-partition/partitions/<id>/.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String dataPath = requiredString(args, "data_path");
                return ok(listPartitionsImpl(dataPath));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String listPartitionsImpl(final String dataPath) throws IOException {
    final Path partitionsDir = Paths.get(dataPath, "raft-partition", "partitions");
    final ArrayNode array = MAPPER.createArrayNode();

    if (!Files.isDirectory(partitionsDir)) {
      final ObjectNode result = MAPPER.createObjectNode();
      result.put("data_path", dataPath);
      result.put("partitions_dir", partitionsDir.toString());
      result.put("exists", false);
      result.set("partitions", array);
      return result.toString();
    }

    try (Stream<Path> stream = Files.list(partitionsDir)) {
      stream
          .filter(Files::isDirectory)
          .filter(p -> isNumeric(p.getFileName().toString()))
          .sorted(Comparator.comparingInt(p -> Integer.parseInt(p.getFileName().toString())))
          .forEach(
              p -> {
                final ObjectNode entry = MAPPER.createObjectNode();
                final String id = p.getFileName().toString();
                entry.put("id", Integer.parseInt(id));
                entry.put("partition_path", p.toAbsolutePath().toString());
                final Path runtime = p.resolve("runtime");
                final boolean hasRuntime = Files.isDirectory(runtime);
                entry.put("has_runtime", hasRuntime);
                entry.put("runtime_path", runtime.toAbsolutePath().toString());
                entry.put("snapshot_count", countSnapshots(p));
                array.add(entry);
              });
    }

    final ObjectNode result = MAPPER.createObjectNode();
    result.put("data_path", dataPath);
    result.put("partitions_dir", partitionsDir.toString());
    result.put("exists", true);
    result.set("partitions", array);
    return result.toString();
  }

  private static int countSnapshots(final Path partitionDir) {
    final Path snapshots = partitionDir.resolve("snapshots");
    if (!Files.isDirectory(snapshots)) {
      return 0;
    }
    try (Stream<Path> stream = Files.list(snapshots)) {
      return (int) stream.filter(Files::isDirectory).count();
    } catch (final IOException e) {
      return 0;
    }
  }

  private static boolean isNumeric(final String s) {
    if (s == null || s.isEmpty()) {
      return false;
    }
    for (int i = 0; i < s.length(); i++) {
      if (!Character.isDigit(s.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private static McpServerFeatures.SyncToolSpecification getPartitionStatus() {
    final JsonSchema schema =
        new JsonSchema(
            "object",
            Map.of(
                "partition_path",
                Map.of(
                    "type",
                    "string",
                    "description",
                    "Path to a partition directory as returned by list_partitions.")),
            List.of("partition_path"),
            false,
            null,
            null);

    final Tool tool =
        Tool.builder()
            .name("get_partition_status")
            .description(
                "Returns the current log and raft status of a partition. Useful as a first "
                    + "step to understand partition health: position range, index range, "
                    + "term, and cluster configuration.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                return ok(getPartitionStatusImpl(partitionPath));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String getPartitionStatusImpl(final String partitionPath) {
    final Path partDir = Paths.get(partitionPath);
    final String log = new LogStatus(partDir).status().toString();
    final String raft = new RaftStatus(partDir).detailsAsJson();
    return "{\"log\":" + log + ",\"raft\":" + raft + "}";
  }

  /** Helper returning a thunk that wraps the result-or-error logic for unit tests. */
  public static CallToolResult listPartitionsCall(final String dataPath) {
    try {
      return ok(listPartitionsImpl(dataPath));
    } catch (final Exception e) {
      return error(e);
    }
  }

  /** Helper returning a thunk that wraps the result-or-error logic for unit tests. */
  public static CallToolResult getPartitionStatusCall(final String partitionPath) {
    try {
      return ok(getPartitionStatusImpl(partitionPath));
    } catch (final Exception e) {
      return error(e);
    }
  }
}
