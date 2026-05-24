# zdb — Zeebe Debug & Inspection Tool

zdb reads Zeebe (Camunda 8) partition data from disk: the embedded RocksDB
runtime state, the segmented Raft log, and the raft meta/config files. It is a
read-only, offline tool intended for debugging customer incidents from a copy
of a broker's data folder.

## Module structure

This is a Maven multi-module project (Java 21, Kotlin backend, JavaFX
frontend):

- `backend/` — Kotlin library exposing the readers (`io.zell.zdb.*`):
  state (`IncidentState`, `InstanceState`, `ProcessState`, `ZeebeDbReader`),
  log (`LogStatus`, `LogSearch`, `LogContentReader`), raft (`RaftStatus`),
  and the `ZeebePaths` helper for resolving `<data>/raft-partition/...`.
- `cli/` — picocli-based CLI (`io.zell.zdb.ZeebeDebugger`) that wraps the
  backend readers as subcommands.
- `frontend/` — JavaFX desktop UI.
- `mcp-server/` — Model Context Protocol server (`io.zell.zdb.mcp.ZdbMcpServer`)
  that exposes the backend readers as MCP tools so an LLM (Claude) can analyse
  customer partition data locally.

## Building

Always run from the repo root.

```sh
mvn package -DskipTests        # quick build, all modules
mvn verify                      # full build + tests (Testcontainers required)
mvn package -pl mcp-server -am -DskipTests   # MCP server only
```

The CLI assembly produces `cli/target/cli-<version>-jar-with-dependencies.jar`
and the MCP server produces `mcp-server/target/zdb-mcp-jar-with-dependencies.jar`.

## Running the CLI

```sh
./zdb --path <partition-path> <command>
```

`<partition-path>` points at the partition directory (e.g.
`<data>/raft-partition/partitions/1`). Available subcommands include
`incident`, `instance`, `process`, `log`, `raft`, and `state`.

## Running the MCP server

```sh
mvn package -pl mcp-server -am -DskipTests
./zdb-mcp
```

The script execs the assembled jar with stdio; the server speaks MCP over
stdin/stdout.

### Configuring Claude Code

Add an entry to `~/.claude/mcp_servers.json` (or whatever Claude Code config
you use):

```json
{
  "mcpServers": {
    "zdb": {
      "command": "/absolute/path/to/zdb/zdb-mcp"
    }
  }
}
```

### Tools exposed

12 read-only tools, all returning JSON text content:

- `list_partitions(data_path)` — discover partition dirs under a Zeebe data
  folder. Always call this first.
- `get_partition_status(partition_path)` — combined log+raft summary.
- `list_processes`, `get_process` — deployed process definitions.
- `list_process_instances`, `get_process_instance` — running instances.
- `list_incidents`, `get_incident` — active incidents (filterable by
  `error_type`, `bpmn_process_id`, `process_definition_key`).
- `get_log_status`, `search_log_by_position`, `search_log_by_index`,
  `list_log_records` — Raft log inspection.
- `list_column_families` — RocksDB column family row counts.

Pagination envelopes: `{"total_fetched":N,"offset":O,"limit":L,"has_more":B,"data":[...]}`.

## Backend API quick reference

State readers take the **runtime** path (`<part>/runtime`). Log/raft readers
take the **partition** path (`<part>`). Use `io.zell.zdb.ZeebePaths.getRuntimePath`
and `getLogPath` to construct them, or `io.zell.zdb.mcp.util.PathResolver`
inside the MCP server (which falls back to the latest snapshot if no live
runtime exists).

Key visitor types:

- `JsonElementVisitor#visit(String json)` — used by `IncidentState.listIncidents`.
- `ZeebeDbReader.JsonValueWithKeyPrefixVisitor#visit(byte[] key, String json)`
  — used by `InstanceState.listProcessInstances`, `ProcessState.listProcesses`,
  etc.

## Tests

- Tests use **Testcontainers** with `io.zeebe:zeebe-test-container` to spin up
  a real Zeebe (Camunda 8) broker, mount its data dir to the host, deploy a
  workload, then run readers against the resulting RocksDB / log files.
- `backend/src/test/kotlin/io/zell/zdb/TestUtils.java` provides
  `createZeebeContainerGreaterOrEquals88(...)` for 8.8+ images and
  `createZeebeContainerBefore85(...)` for older images. The MCP server test
  inlines its own container setup to avoid pulling in test scope from
  `backend`.
- Per-version test classes live in `backend/src/test/kotlin/io/zell/zdb/v8x/`.

## Zeebe data layout

```
<data>/                                    # what users mount/copy
  raft-partition/
    partitions/
      1/                                   # partition dir (numeric name)
        runtime/                           # live RocksDB state
        snapshots/<position>-<term>/       # snapshot RocksDB state
        raft-partition-partition-1.meta    # raft meta
        raft-partition-partition-1.conf    # raft config
        raft-partition-partition-1-1.log   # raft log segment(s)
```

The MCP `partition_path` argument always refers to the `partitions/<id>/`
directory (not the runtime). State tools resolve the runtime internally; log
and raft tools take the partition dir directly.
