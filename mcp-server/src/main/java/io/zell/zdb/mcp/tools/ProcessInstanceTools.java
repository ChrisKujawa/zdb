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
import static io.zell.zdb.mcp.util.ToolUtils.optionalString;
import static io.zell.zdb.mcp.util.ToolUtils.requiredLong;
import static io.zell.zdb.mcp.util.ToolUtils.requiredString;

import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.JsonSchema;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import io.zell.zdb.mcp.util.PaginatedResult;
import io.zell.zdb.mcp.util.PathResolver;
import io.zell.zdb.state.instance.InstanceState;
import io.zell.zdb.state.instance.ProcessInstanceRecordDetails;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/** MCP tool specifications for inspecting process instances. */
public final class ProcessInstanceTools {

  private static final int DEFAULT_LIMIT = 50;
  private static final int MAX_LIMIT = 500;

  private ProcessInstanceTools() {}

  public static List<McpServerFeatures.SyncToolSpecification> specs() {
    return List.of(listProcessInstances(), getProcessInstance());
  }

  private static McpServerFeatures.SyncToolSpecification listProcessInstances() {
    final JsonSchema schema =
        new JsonSchema(
            "object",
            Map.of(
                "partition_path",
                    Map.of(
                        "type", "string",
                        "description",
                            "Path to a partition directory as returned by list_partitions."),
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
            .name("list_process_instances")
            .description(
                "Lists process instances (root process level only, not sub-elements). "
                    + "Filter by bpmn_process_id or process_definition_key to narrow "
                    + "results. Use limit/offset to paginate. Each result includes "
                    + "processInstanceKey, bpmnProcessId, state, and processDefinitionKey.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                final String bpmnProcessId = optionalString(args, "bpmn_process_id");
                final Long processDefinitionKey = optionalLong(args, "process_definition_key");
                final int limit = Math.min(MAX_LIMIT, intOrDefault(args, "limit", DEFAULT_LIMIT));
                final int offset = intOrDefault(args, "offset", 0);
                return ok(
                    listProcessInstancesImpl(
                        partitionPath, bpmnProcessId, processDefinitionKey, offset, limit));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String listProcessInstancesImpl(
      final String partitionPath,
      final String bpmnProcessId,
      final Long processDefinitionKey,
      final int offset,
      final int limit) {
    final Path runtime = PathResolver.runtimePath(partitionPath);
    final Predicate<ProcessInstanceRecordDetails> predicate =
        details -> {
          if (bpmnProcessId != null && !bpmnProcessId.equals(details.getBpmnProcessId())) {
            return false;
          }
          if (processDefinitionKey != null
              && processDefinitionKey != details.getProcessDefinitionKey()) {
            return false;
          }
          return true;
        };

    final List<String> items = new ArrayList<>();
    new InstanceState(runtime)
        .listProcessInstances(predicate, (key, valueJson) -> items.add(valueJson));
    return PaginatedResult.buildPageJson(items, offset, limit);
  }

  private static McpServerFeatures.SyncToolSpecification getProcessInstance() {
    final JsonSchema schema =
        new JsonSchema(
            "object",
            Map.of(
                "partition_path",
                    Map.of(
                        "type", "string",
                        "description",
                            "Path to a partition directory as returned by list_partitions."),
                "element_instance_key",
                    Map.of(
                        "type", "integer",
                        "description", "Element instance key (process instance or sub-element).")),
            List.of("partition_path", "element_instance_key"),
            false,
            null,
            null);

    final Tool tool =
        Tool.builder()
            .name("get_process_instance")
            .description(
                "Returns full details of a specific element instance (process or "
                    + "sub-element) by its key. Returns child counts, states, and the full "
                    + "element record.")
            .inputSchema(schema)
            .build();

    return McpServerFeatures.SyncToolSpecification.builder()
        .tool(tool)
        .callHandler(
            (exchange, request) -> {
              try {
                final Map<String, Object> args = request.arguments();
                final String partitionPath = requiredString(args, "partition_path");
                final long key = requiredLong(args, "element_instance_key");
                return ok(getProcessInstanceImpl(partitionPath, key));
              } catch (final Exception e) {
                return error(e);
              }
            })
        .build();
  }

  static String getProcessInstanceImpl(final String partitionPath, final long key) {
    final Path runtime = PathResolver.runtimePath(partitionPath);
    return new InstanceState(runtime).getInstance(key);
  }

  /** Direct test entrypoint. */
  public static CallToolResult listProcessInstancesCall(
      final String partitionPath,
      final String bpmnProcessId,
      final Long processDefinitionKey,
      final int offset,
      final int limit) {
    try {
      return ok(
          listProcessInstancesImpl(
              partitionPath, bpmnProcessId, processDefinitionKey, offset, limit));
    } catch (final Exception e) {
      return error(e);
    }
  }

  /** Direct test entrypoint. */
  public static CallToolResult getProcessInstanceCall(final String partitionPath, final long key) {
    try {
      return ok(getProcessInstanceImpl(partitionPath, key));
    } catch (final Exception e) {
      return error(e);
    }
  }
}
