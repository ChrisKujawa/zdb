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
import static io.zell.zdb.mcp.util.ToolUtils.ok;
import static io.zell.zdb.mcp.util.ToolUtils.requiredString;

import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.JsonSchema;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.zell.zdb.mcp.util.PathResolver;
import io.zell.zdb.state.ZeebeDbReader;
import java.util.List;
import java.util.Map;

/** MCP tool specifications for inspecting RocksDB column family statistics. */
public final class ColumnFamilyTools {

  private ColumnFamilyTools() {}

  public static List<McpServerFeatures.SyncToolSpecification> specs() {
    return List.of(listColumnFamilies());
  }

  private static McpServerFeatures.SyncToolSpecification listColumnFamilies() {
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
            .name("list_column_families")
            .description(
                "Returns row counts per RocksDB column family. Useful for understanding "
                    + "partition state volume. Does not return full data — just counts. To "
                    + "inspect data in a specific column family, use the state commands.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                return ok(listColumnFamiliesImpl(partitionPath));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String listColumnFamiliesImpl(final String partitionPath) {
    return new ZeebeDbReader(PathResolver.runtimePath(partitionPath)).stateStatisticsAsJsonString();
  }

  /** Direct test entrypoint. */
  public static CallToolResult listColumnFamiliesCall(final String partitionPath) {
    try {
      return ok(listColumnFamiliesImpl(partitionPath));
    } catch (final Exception e) {
      return error(e);
    }
  }
}
