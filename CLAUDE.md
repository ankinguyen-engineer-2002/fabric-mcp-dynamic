# Fabric MCP Dynamic — Custom MCP Server

## What
Custom MCP server wrapping Azure REST API + OneLake into reusable tools for Claude Code.
Installable globally — works across any project without dependency on external MCP servers.

## Tech Stack
- Python 3.10+ with `fastmcp` (MCP SDK)
- `pyarrow` for parquet/Delta table reads
- `httpx` for async HTTP to OneLake + Fabric API
- Auth: `az login` tokens (no Service Principal needed)

## Project Structure
```
fabric-mcp-dynamic/
├── pyproject.toml
├── src/
│   └── fabric_mcp_dynamic/
│       ├── __init__.py
│       ├── server.py            ← MCP entry point
│       ├── auth.py              ← az cli token management
│       ├── config.py            ← workspace/env config
│       ├── tools/
│       │   ├── discovery.py     ← scan_workspace, get_metadata, get_pipeline_def, get_notebook_list
│       │   ├── data_ops.py      ← query_lakehouse, query_warehouse, read_delta_table, get_table_schema, get_row_count, preview_table
│       │   ├── pipeline.py      ← trigger_pipeline, get_pipeline_status, get_pipeline_history, cancel_pipeline
│       │   ├── data_quality.py  ← run_dq_check, get_dq_results, validate_row_counts, check_freshness
│       │   ├── lineage.py       ← get_data_dictionary, get_lineage, update_metadata
│       │   └── viz.py           ← generate_viz, deploy_github_pages, preview_local
│       └── utils/
│           ├── delta.py         ← checkpoint replay logic (proven from MCP Fabric project)
│           ├── onelake.py       ← OneLake REST client
│           └── fabric_api.py    ← Fabric API REST client
└── tests/
```

## 27 Tools (7 categories)
1. **Discovery** (4): scan_workspace, get_metadata, get_pipeline_def, get_notebook_list
2. **Data Ops** (6): query_lakehouse, query_warehouse, read_delta_table, get_table_schema, get_row_count, preview_table
3. **Pipeline** (4): trigger_pipeline, get_pipeline_status, get_pipeline_history, cancel_pipeline
4. **Data Quality** (4): run_dq_check, get_dq_results, validate_row_counts, check_freshness
5. **Metadata & Lineage** (3): get_data_dictionary, get_lineage, update_metadata
6. **Visualization** (3): generate_viz, deploy_github_pages, preview_local
7. **Environment** (3): health_check, switch_env, get_config/set_config

## Target Config (after install)
```json
// ~/.claude.json
{
  "mcpServers": {
    "fabric-dynamic": {
      "type": "stdio",
      "command": "python",
      "args": ["-m", "fabric_mcp_dynamic"],
      "env": {
        "FABRIC_WORKSPACE_ID": "c8d9fc83-18b6-4e1d-8264-0b49eed36fe0",
        "FABRIC_LAKEHOUSE_ID": "62a3081e-4093-4f46-856c-f50aa58732fa",
        "FABRIC_WAREHOUSE_ID": "e146ffe2-d907-46a7-9b7e-3e739a31b24e",
        "FABRIC_ENV": "dev"
      }
    }
  }
}
```

## Origin
Logic ported from `/Users/MAC/Documents/MCP Fabric` project — proven Azure REST API + delta log replay approach.
See that project's `.agents/skills/fabric_pipeline_scan/SKILL.md` for the original workflow.

## Complements (not replaces)
- `@microsoft/fabric-mcp` — official Microsoft, offline docs/code-gen context
- `@wcg-hieule/fabric-mcp` — community, live with Service Principal

This server's unique value: DQ, Viz, Lineage, Delta replay, Cross-layer validation.
