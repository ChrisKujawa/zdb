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
import static io.zell.zdb.mcp.util.ToolUtils.intOrDefault;
import static io.zell.zdb.mcp.util.ToolUtils.ok;
import static io.zell.zdb.mcp.util.ToolUtils.optionalLong;
import static io.zell.zdb.mcp.util.ToolUtils.optionalString;
import static io.zell.zdb.mcp.util.ToolUtils.requiredLong;
import static io.zell.zdb.mcp.util.ToolUtils.requiredString;

import com.fasterxml.jackson.databind.JsonNode;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.JsonSchema;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.zell.zdb.mcp.util.PaginatedResult;
import io.zell.zdb.mcp.util.PathResolver;
import io.zell.zdb.state.incident.IncidentState;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** MCP tool specifications for inspecting incidents. */
public final class IncidentTools {

  private static final int DEFAULT_LIMIT = 50;
  private static final int MAX_LIMIT = 500;

  private IncidentTools() {}

  public static List<McpServerFeatures.SyncToolSpecification> specs() {
    return List.of(listIncidents(), getIncident());
  }

  private static McpServerFeatures.SyncToolSpecification listIncidents() {
    final JsonSchema schema =
        new JsonSchema(
            "object",
            Map.of(
                "partition_path",
                    Map.of(
                        "type", "string",
                        "description",
                            "Path to a partition directory as returned by list_partitions."),
                "error_type",
                    Map.of(
                        "type",
                        "string",
                        "description",
                        "Optional error type filter (e.g. JOB_NO_RETRIES, "
                            + "EXTRACT_VALUE_ERROR, CALLED_ELEMENT_ERROR, "
                            + "UNHANDLED_ERROR_EVENT, MESSAGE_SIZE_EXCEEDED)."),
                "bpmn_process_id",
                    Map.of(
                        "type", "string",
                        "description", "Optional BPMN process ID filter (exact match)."),
                "process_definition_key",
                    Map.of(
                        "type", "integer",
                        "description", "Optional processDefinitionKey filter (exact match)."),
                "limit",
                    Map.of(
                        "type", "integer",
                        "description", "Maximum number of items per page. Default 50, max 500."),
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
            .name("list_incidents")
            .description(
                "Lists all active incidents in the partition. Filter by error_type "
                    + "(e.g. JOB_NO_RETRIES, EXTRACT_VALUE_ERROR, CALLED_ELEMENT_ERROR, "
                    + "UNHANDLED_ERROR_EVENT, MESSAGE_SIZE_EXCEEDED), bpmn_process_id, or "
                    + "process_definition_key. Use limit/offset to paginate. Start with a "
                    + "small limit to understand the volume.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                final String errorType = optionalString(args, "error_type");
                final String bpmnProcessId = optionalString(args, "bpmn_process_id");
                final Long processDefinitionKey = optionalLong(args, "process_definition_key");
                final int limit = Math.min(MAX_LIMIT, intOrDefault(args, "limit", DEFAULT_LIMIT));
                final int offset = intOrDefault(args, "offset", 0);
                return ok(
                    listIncidentsImpl(
                        partitionPath,
                        errorType,
                        bpmnProcessId,
                        processDefinitionKey,
                        offset,
                        limit));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String listIncidentsImpl(
      final String partitionPath,
      final String errorType,
      final String bpmnProcessId,
      final Long processDefinitionKey,
      final int offset,
      final int limit) {
    final Path runtime = PathResolver.runtimePath(partitionPath);
    final List<String> items = new ArrayList<>();
    new IncidentState(runtime)
        .listIncidents(
            jsonElement -> {
              if (matchesFilters(jsonElement, errorType, bpmnProcessId, processDefinitionKey)) {
                items.add(jsonElement);
              }
            });
    return PaginatedResult.buildPageJson(items, offset, limit);
  }

  private static boolean matchesFilters(
      final String incidentJson,
      final String errorType,
      final String bpmnProcessId,
      final Long processDefinitionKey) {
    if (errorType == null && bpmnProcessId == null && processDefinitionKey == null) {
      return true;
    }
    try {
      final JsonNode root = MAPPER.readTree(incidentJson);
      final JsonNode value = root.path("value");
      if (errorType != null && !errorType.equals(value.path("errorType").asText(null))) {
        return false;
      }
      if (bpmnProcessId != null
          && !bpmnProcessId.equals(value.path("bpmnProcessId").asText(null))) {
        return false;
      }
      if (processDefinitionKey != null) {
        final JsonNode pdk = value.path("processDefinitionKey");
        if (!pdk.isNumber() || pdk.asLong() != processDefinitionKey) {
          return false;
        }
      }
      return true;
    } catch (final Exception e) {
      return false;
    }
  }

  private static McpServerFeatures.SyncToolSpecification getIncident() {
    final JsonSchema schema =
        new JsonSchema(
            "object",
            Map.of(
                "partition_path",
                    Map.of(
                        "type", "string",
                        "description",
                            "Path to a partition directory as returned by list_partitions."),
                "incident_key",
                    Map.of(
                        "type", "integer",
                        "description", "Incident key.")),
            List.of("partition_path", "incident_key"),
            false,
            null,
            null);

    final Tool tool =
        Tool.builder()
            .name("get_incident")
            .description("Returns full details of a specific incident by key.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                final long incidentKey = requiredLong(args, "incident_key");
                return ok(getIncidentImpl(partitionPath, incidentKey));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String getIncidentImpl(final String partitionPath, final long incidentKey) {
    final Path runtime = PathResolver.runtimePath(partitionPath);
    return new IncidentState(runtime).incidentDetails(incidentKey);
  }

  /** Direct test entrypoint. */
  public static CallToolResult listIncidentsCall(
      final String partitionPath,
      final String errorType,
      final String bpmnProcessId,
      final Long processDefinitionKey,
      final int offset,
      final int limit) {
    try {
      return ok(
          listIncidentsImpl(
              partitionPath, errorType, bpmnProcessId, processDefinitionKey, offset, limit));
    } catch (final Exception e) {
      return error(e);
    }
  }

  /** Direct test entrypoint. */
  public static CallToolResult getIncidentCall(final String partitionPath, final long incidentKey) {
    try {
      return ok(getIncidentImpl(partitionPath, incidentKey));
    } catch (final Exception e) {
      return error(e);
    }
  }
}
