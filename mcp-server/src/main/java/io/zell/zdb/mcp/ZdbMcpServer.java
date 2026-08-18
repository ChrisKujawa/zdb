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

import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import io.zell.zdb.mcp.tools.ColumnFamilyTools;
import io.zell.zdb.mcp.tools.IncidentTools;
import io.zell.zdb.mcp.tools.LogTools;
import io.zell.zdb.mcp.tools.PartitionTools;
import io.zell.zdb.mcp.tools.ProcessInstanceTools;
import io.zell.zdb.mcp.tools.ProcessTools;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for the zdb MCP server. Reads/writes JSON-RPC messages over stdio and exposes the zdb
 * backend's read-only inspection capabilities as MCP tools.
 */
public final class ZdbMcpServer {

  private static final String SERVER_NAME = "zdb-mcp";
  private static final String SERVER_VERSION = "2.7.0";

  private ZdbMcpServer() {}

  public static void main(final String[] args) throws InterruptedException {
    final StdioServerTransportProvider transport =
        new StdioServerTransportProvider(McpJsonDefaults.getMapper());

    final List<McpServerFeatures.SyncToolSpecification> tools = new ArrayList<>();
    tools.addAll(PartitionTools.specs());
    tools.addAll(ProcessTools.specs());
    tools.addAll(ProcessInstanceTools.specs());
    tools.addAll(IncidentTools.specs());
    tools.addAll(LogTools.specs());
    tools.addAll(ColumnFamilyTools.specs());

    final McpSyncServer server =
        McpServer.sync(transport)
            .serverInfo(SERVER_NAME, SERVER_VERSION)
            .capabilities(ServerCapabilities.builder().tools(true).build())
            .tools(tools)
            .build();

    // Keep the JVM alive while the stdio transport is running.
    try {
      Thread.currentThread().join();
    } finally {
      server.close();
    }
  }
}
