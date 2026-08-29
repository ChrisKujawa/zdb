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

import static io.zell.zdb.mcp.util.ToolUtils.error;
import static io.zell.zdb.mcp.util.ToolUtils.intOrDefault;
import static io.zell.zdb.mcp.util.ToolUtils.ok;
import static io.zell.zdb.mcp.util.ToolUtils.requiredLong;
import static io.zell.zdb.mcp.util.ToolUtils.requiredString;

import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.JsonSchema;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.zell.zdb.mcp.util.PaginatedResult;
import io.zell.zdb.mcp.util.PathResolver;
import io.zell.zdb.state.process.ProcessState;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** MCP tool specifications for inspecting deployed process definitions. */
public final class ProcessTools {

  private static final int DEFAULT_LIMIT = 50;
  private static final int MAX_LIMIT = 500;

  private ProcessTools() {}

  public static List<McpServerFeatures.SyncToolSpecification> specs() {
    return List.of(listProcesses(), getProcess());
  }

  private static McpServerFeatures.SyncToolSpecification listProcesses() {
    final JsonSchema schema =
        new JsonSchema(
            "object",
            Map.of(
                "partition_path",
                    Map.of(
                        "type", "string",
                        "description",
                            "Path to a partition directory as returned by list_partitions."),
                "limit",
                    Map.of(
                        "type",
                        "integer",
                        "description",
                        "Maximum number of items per page. Default 50, max 500."),
                "offset",
                    Map.of(
                        "type", "integer",
                        "description", "Number of items to skip. Default 0.")),
            List.of("partition_path"),
            false,
            null,
            null);

    final Tool tool =
        Tool.builder()
            .name("list_processes")
            .description(
                "Lists deployed process definitions in the partition. Use limit/offset to "
                    + "paginate. Returns process key, BPMN process ID, version, and "
                    + "resource name. To investigate a specific process, use get_process "
                    + "with the processDefinitionKey.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                final int limit = Math.min(MAX_LIMIT, intOrDefault(args, "limit", DEFAULT_LIMIT));
                final int offset = intOrDefault(args, "offset", 0);
                return ok(listProcessesImpl(partitionPath, offset, limit));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String listProcessesImpl(final String partitionPath, final int offset, final int limit) {
    final Path runtime = PathResolver.runtimePath(partitionPath);
    final List<String> items = new ArrayList<>();
    new ProcessState(runtime).listProcesses((key, valueJson) -> items.add(valueJson));
    return PaginatedResult.buildPageJson(items, offset, limit);
  }

  private static McpServerFeatures.SyncToolSpecification getProcess() {
    final JsonSchema schema =
        new JsonSchema(
            "object",
            Map.of(
                "partition_path",
                    Map.of(
                        "type", "string",
                        "description",
                            "Path to a partition directory as returned by list_partitions."),
                "process_definition_key",
                    Map.of(
                        "type", "integer",
                        "description", "The processDefinitionKey of the process to fetch.")),
            List.of("partition_path", "process_definition_key"),
            false,
            null,
            null);

    final Tool tool =
        Tool.builder()
            .name("get_process")
            .description(
                "Returns full details of a specific process definition, including BPMN "
                    + "XML. The BPMN resource may be large — consider using list_processes "
                    + "first to identify the key.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                final long key = requiredLong(args, "process_definition_key");
                return ok(getProcessImpl(partitionPath, key));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String getProcessImpl(final String partitionPath, final long processDefinitionKey) {
    final Path runtime = PathResolver.runtimePath(partitionPath);
    final List<String> matches = new ArrayList<>();
    new ProcessState(runtime)
        .processDetails(processDefinitionKey, (key, valueJson) -> matches.add(valueJson));
    if (matches.isEmpty()) {
      return "null";
    }
    if (matches.size() == 1) {
      return matches.get(0);
    }
    final StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < matches.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(matches.get(i));
    }
    sb.append("]");
    return sb.toString();
  }

  /** Direct test entrypoint. */
  public static CallToolResult listProcessesCall(
      final String partitionPath, final int offset, final int limit) {
    try {
      return ok(listProcessesImpl(partitionPath, offset, limit));
    } catch (final Exception e) {
      return error(e);
    }
  }

  /** Direct test entrypoint. */
  public static CallToolResult getProcessCall(
      final String partitionPath, final long processDefinitionKey) {
    try {
      return ok(getProcessImpl(partitionPath, processDefinitionKey));
    } catch (final Exception e) {
      return error(e);
    }
  }
}
