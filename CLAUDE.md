
# Fabric MCP Dynamic — Custom MCP Server

## What
Custom MCP server wrapping Azure REST API + OneLake into reusable tools for Claude Code.
Installable via `uvx fabric-mcp-dynamic` — works across any project.

## Tech Stack
- Python 3.10+ with `fastmcp` (MCP SDK)
- `pyarrow` for parquet/Delta table reads
- `httpx` for async HTTP to OneLake + Fabric API
- Auth: `az login` tokens (no Service Principal needed)

## Project Structure
```
fabric-mcp-dynamic/
├── pyproject.toml
├── LICENSE
├── README.md
├── src/
│   └── fabric_mcp_dynamic/
│       ├── __init__.py
│       ├── __main__.py          ← python -m entry point
│       ├── server.py            ← MCP entry point (FastMCP)
│       ├── auth.py              ← az cli token management
│       ├── config.py            ← workspace/env config via env vars
│       ├── tools/
│       │   ├── discovery.py     ← scan_workspace, get_metadata, get_pipeline_def, get_notebook_list
│       │   ├── data_ops.py      ← query_lakehouse, query_warehouse, read_delta_table, get_table_schema, get_row_count, preview_table
│       │   ├── pipeline.py      ← trigger_pipeline, get_pipeline_status, get_pipeline_history, cancel_pipeline
│       │   ├── data_quality.py  ← run_dq_check, get_dq_results, validate_row_counts, check_freshness
│       │   ├── lineage.py       ← get_data_dictionary, get_lineage, update_metadata
│       │   ├── local_lineage.py ← discover_lineage, trace_table, scan_local_folder
│       │   ├── viz.py           ← generate_viz, deploy_github_pages, preview_local
│       │   └── environment.py   ← health_check, switch_env, get_config, set_config
│       └── utils/
│           ├── delta.py         ← Delta checkpoint replay logic
│           ├── onelake.py       ← OneLake REST client
│           └── fabric_api.py    ← Fabric API REST client
└── tests/
```

## 31 Tools (8 categories)
1. **Discovery** (4): scan_workspace, get_metadata, get_pipeline_def, get_notebook_list
2. **Data Ops** (6): query_lakehouse, query_warehouse, read_delta_table, get_table_schema, get_row_count, preview_table
3. **Pipeline** (4): trigger_pipeline, get_pipeline_status, get_pipeline_history, cancel_pipeline
4. **Data Quality** (4): run_dq_check, get_dq_results, validate_row_counts, check_freshness
5. **Metadata & Lineage** (3): get_data_dictionary, get_lineage, update_metadata
6. **Lineage Discovery** (3): discover_lineage, trace_table, scan_local_folder
7. **Visualization** (3): generate_viz, deploy_github_pages, preview_local
8. **Environment** (4): health_check, switch_env, get_current_config, set_current_config

## Quick Start (after `pip install fabric-mcp-dynamic` or `uvx`)
```json
// ~/.claude.json or .claude/settings.local.json
{
  "mcpServers": {
    "fabric-dynamic": {
      "type": "stdio",
      "command": "uvx",
      "args": ["fabric-mcp-dynamic"],
      "env": {
        "FABRIC_WORKSPACE_ID": "<your-workspace-guid>",
        "FABRIC_LAKEHOUSE_ID": "<your-lakehouse-guid>",
        "FABRIC_WAREHOUSE_ID": "<your-warehouse-guid>",
        "FABRIC_ENV": "dev"
      }
    }
  }
}
```

## Prerequisite
- `az login` — Azure CLI must be logged in (tokens are fetched at runtime)

## Complements (not replaces)
- `@anthropic/microsoft-fabric-mcp` — official Microsoft, offline docs/code-gen context
- `@wcg-hieule/fabric-mcp` — community, live with Service Principal

This server's unique value: DQ, Viz, Lineage Discovery, Delta replay, Cross-layer validation, metadata-driven pipeline orchestration.
