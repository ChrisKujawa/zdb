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
import static io.zell.zdb.mcp.util.ToolUtils.optionalLong;
import static io.zell.zdb.mcp.util.ToolUtils.requiredLong;
import static io.zell.zdb.mcp.util.ToolUtils.requiredString;

import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.JsonSchema;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.zell.zdb.log.LogContentReader;
import io.zell.zdb.log.LogSearch;
import io.zell.zdb.log.LogStatus;
import io.zell.zdb.log.records.PersistedRecord;
import io.zell.zdb.mcp.util.PaginatedResult;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** MCP tool specifications for inspecting the Raft log. */
public final class LogTools {

  private static final int DEFAULT_LIMIT = 20;
  private static final int MAX_LIMIT = 200;

  private LogTools() {}

  public static List<McpServerFeatures.SyncToolSpecification> specs() {
    return List.of(getLogStatus(), searchLogByPosition(), searchLogByIndex(), listLogRecords());
  }

  private static McpServerFeatures.SyncToolSpecification getLogStatus() {
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
            .name("get_log_status")
            .description(
                "Returns log statistics: position range, index range, and highest term. "
                    + "Use this to understand what data is in the log before using "
                    + "list_log_records or search_log_by_position.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                return ok(getLogStatusImpl(partitionPath));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String getLogStatusImpl(final String partitionPath) {
    return new LogStatus(Paths.get(partitionPath)).status().toString();
  }

  private static McpServerFeatures.SyncToolSpecification searchLogByPosition() {
    final JsonSchema schema =
        new JsonSchema(
            "object",
            Map.of(
                "partition_path",
                    Map.of(
                        "type", "string",
                        "description",
                            "Path to a partition directory as returned by list_partitions."),
                "position",
                    Map.of(
                        "type", "integer",
                        "description", "Application-level position (ASQN) to look up.")),
            List.of("partition_path", "position"),
            false,
            null,
            null);

    final Tool tool =
        Tool.builder()
            .name("search_log_by_position")
            .description(
                "Finds a specific log record by application-level position (ASQN). "
                    + "Returns the full record including intent, value type, and record "
                    + "value. Use get_log_status first to find valid position ranges.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                final long position = requiredLong(args, "position");
                return ok(searchLogByPositionImpl(partitionPath, position));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String searchLogByPositionImpl(final String partitionPath, final long position) {
    final var record = new LogSearch(Paths.get(partitionPath)).searchPosition(position);
    return record == null ? "null" : record.toString();
  }

  private static McpServerFeatures.SyncToolSpecification searchLogByIndex() {
    final JsonSchema schema =
        new JsonSchema(
            "object",
            Map.of(
                "partition_path",
                    Map.of(
                        "type", "string",
                        "description",
                            "Path to a partition directory as returned by list_partitions."),
                "index",
                    Map.of(
                        "type", "integer",
                        "description", "Raft log index to look up.")),
            List.of("partition_path", "index"),
            false,
            null,
            null);

    final Tool tool =
        Tool.builder()
            .name("search_log_by_index")
            .description(
                "Finds a log entry by raft index (may be an ApplicationRecord or a "
                    + "RaftRecord). Use get_log_status to find valid index ranges.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                final long index = requiredLong(args, "index");
                return ok(searchLogByIndexImpl(partitionPath, index));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String searchLogByIndexImpl(final String partitionPath, final long index) {
    final var record = new LogSearch(Paths.get(partitionPath)).searchIndex(index);
    return record == null ? "null" : record.toString();
  }

  private static McpServerFeatures.SyncToolSpecification listLogRecords() {
    final JsonSchema schema =
        new JsonSchema(
            "object",
            Map.of(
                "partition_path",
                    Map.of(
                        "type", "string",
                        "description",
                            "Path to a partition directory as returned by list_partitions."),
                "from_position",
                    Map.of(
                        "type", "integer",
                        "description", "Optional starting position (ASQN) for the scan."),
                "to_position",
                    Map.of(
                        "type", "integer",
                        "description", "Optional ending position (ASQN) for the scan (exclusive)."),
                "process_instance_key",
                    Map.of(
                        "type", "integer",
                        "description", "Optional process instance key filter."),
                "limit",
                    Map.of(
                        "type", "integer",
                        "description", "Maximum number of items per page. Default 20, max 200."),
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
            .name("list_log_records")
            .description(
                "Streams log records with optional filters. WARNING: the log can be very "
                    + "large (millions of records). Always use from_position/to_position or "
                    + "process_instance_key to narrow the range, and start with a small "
                    + "limit (10-20). Each record includes position, intent, valueType, "
                    + "key, and recordValue. Consider using search_log_by_position or "
                    + "search_log_by_index for point lookups instead.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                final Long fromPosition = optionalLong(args, "from_position");
                final Long toPosition = optionalLong(args, "to_position");
                final Long processInstanceKey = optionalLong(args, "process_instance_key");
                final int limit = Math.min(MAX_LIMIT, intOrDefault(args, "limit", DEFAULT_LIMIT));
                final int offset = intOrDefault(args, "offset", 0);
                return ok(
                    listLogRecordsImpl(
                        partitionPath,
                        fromPosition,
                        toPosition,
                        processInstanceKey,
                        offset,
                        limit));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String listLogRecordsImpl(
      final String partitionPath,
      final Long fromPosition,
      final Long toPosition,
      final Long processInstanceKey,
      final int offset,
      final int limit) {
    final Path partDir = Paths.get(partitionPath);
    final LogContentReader reader = new LogContentReader(partDir);
    if (fromPosition != null) {
      reader.seekToPosition(fromPosition);
    }
    if (toPosition != null) {
      reader.limitToPosition(toPosition);
    }
    if (processInstanceKey != null) {
      reader.filterForProcessInstance(processInstanceKey);
    }

    final int safeOffset = Math.max(0, offset);
    final int safeLimit = Math.max(0, limit);
    final int cap = safeOffset + safeLimit;
    final List<String> collected = new ArrayList<>();
    int collectedCount = 0;
    while (reader.hasNext() && collectedCount < cap) {
      final PersistedRecord record = reader.next();
      collected.add(record.toString());
      collectedCount++;
    }

    return PaginatedResult.buildPageJson(collected, safeOffset, safeLimit);
  }

  /** Direct test entrypoint. */
  public static CallToolResult getLogStatusCall(final String partitionPath) {
    try {
      return ok(getLogStatusImpl(partitionPath));
    } catch (final Exception e) {
      return error(e);
    }
  }

  /** Direct test entrypoint. */
  public static CallToolResult searchLogByPositionCall(
      final String partitionPath, final long position) {
    try {
      return ok(searchLogByPositionImpl(partitionPath, position));
    } catch (final Exception e) {
      return error(e);
    }
  }

  /** Direct test entrypoint. */
  public static CallToolResult searchLogByIndexCall(final String partitionPath, final long index) {
    try {
      return ok(searchLogByIndexImpl(partitionPath, index));
    } catch (final Exception e) {
      return error(e);
    }
  }

  /** Direct test entrypoint. */
  public static CallToolResult listLogRecordsCall(
      final String partitionPath,
      final Long fromPosition,
      final Long toPosition,
      final Long processInstanceKey,
      final int offset,
      final int limit) {
    try {
      return ok(
          listLogRecordsImpl(
              partitionPath, fromPosition, toPosition, processInstanceKey, offset, limit));
    } catch (final Exception e) {
      return error(e);
    }
  }
}
