# Fabric MCP Dynamic

Custom MCP server wrapping **Azure REST API + OneLake** into reusable tools for Claude Code.
Works across any project — no dependency on external MCP servers.

## Why This Exists

| Server | Auth | Live Data | DQ | Lineage | Delta Replay | Viz |
|--------|------|-----------|-----|---------|--------------|-----|
| `@microsoft/fabric-mcp` (official) | N/A | No (offline docs) | No | No | No | No |
| `@wcg-hieule/fabric-mcp` (community) | Service Principal | Yes | No | No | No | No |
| **`fabric-dynamic` (this)** | `az login` | Yes | Yes | Yes | Yes | Yes |

Complements the official and community servers — unique value: **DQ checks, Lineage, Delta log replay, D3.js Visualization, Cross-layer validation**.

## 27 Tools (7 Categories)

| Category | Tools | Description |
|----------|-------|-------------|
| **Discovery** (4) | `scan_workspace`, `get_metadata`, `get_pipeline_def`, `get_notebook_list` | Explore workspace items |
| **Data Ops** (6) | `query_lakehouse`, `query_warehouse`, `read_delta_table`, `get_table_schema`, `get_row_count`, `preview_table` | Read and query data |
| **Pipeline** (4) | `trigger_pipeline`, `get_pipeline_status`, `get_pipeline_history`, `cancel_pipeline` | Orchestrate pipelines |
| **Data Quality** (4) | `run_dq_check`, `get_dq_results`, `validate_row_counts`, `check_freshness` | Validate data quality |
| **Lineage** (3) | `get_data_dictionary`, `get_lineage`, `update_metadata` | Track data lineage |
| **Visualization** (3) | `generate_viz`, `deploy_github_pages`, `preview_local` | D3.js pipeline visualization |
| **Environment** (3) | `health_check`, `switch_env`, `get_current_config`/`set_current_config` | Manage connections |

## Tech Stack

- **Python 3.10+** with [`fastmcp`](https://github.com/jlowin/fastmcp) (MCP SDK)
- **PyArrow** for parquet/Delta table reads (checkpoint replay)
- **httpx** for async HTTP to OneLake + Fabric REST API
- **Auth**: `az login` tokens — no Service Principal needed

## Setup

### Prerequisites

```bash
# Azure CLI authenticated
az login
az account show  # verify correct subscription
```

### Install

```bash
# Clone
git clone https://github.com/ankinguyen-engineer-2002/fabric-mcp-dynamic.git
cd fabric-mcp-dynamic

# Install
pip install -e .
```

### Configure Claude Code

Add to `~/.claude.json`:

```json
{
  "mcpServers": {
    "fabric-dynamic": {
      "type": "stdio",
      "command": "python",
      "args": ["-m", "fabric_mcp_dynamic"],
      "env": {
        "FABRIC_WORKSPACE_ID": "<your-workspace-id>",
        "FABRIC_LAKEHOUSE_ID": "<your-lakehouse-id>",
        "FABRIC_WAREHOUSE_ID": "<your-warehouse-id>",
        "FABRIC_ENV": "dev"
      }
    }
  }
}
```

Replace the IDs with your own Microsoft Fabric workspace/lakehouse/warehouse GUIDs.

### Verify

In Claude Code, run:
```
health_check
```
Should return: auth OK, workspace reachable.

## Usage

### Direct tool calls

Ask Claude Code naturally:

> "Scan my Fabric workspace and show me all items"

> "Read the Delta table utl_pipeline_metadata and show layer breakdown"

> "Run DQ checks and validate row counts across all layers"

### Full pipeline scan (Skill)

Copy the skill file to your Claude Code commands:

```bash
cp skills/analytics_fabric_mcp_dynamic.md ~/.claude/commands/
```

Then in any Claude Code session:

```
/analytics_fabric_mcp_dynamic
```

This runs the full 6-phase workflow: auth check, workspace scan, metadata extraction, DQ validation, visualization, and deployment.

## Project Structure

```
fabric-mcp-dynamic/
├── pyproject.toml
├── src/
│   └── fabric_mcp_dynamic/
│       ├── __init__.py
│       ├── server.py              # MCP entry point
│       ├── auth.py                # az cli token management
│       ├── config.py              # workspace/env config
│       ├── tools/
│       │   ├── discovery.py       # scan_workspace, get_metadata, ...
│       │   ├── data_ops.py        # query_lakehouse, read_delta_table, ...
│       │   ├── pipeline.py        # trigger_pipeline, get_pipeline_status, ...
│       │   ├── data_quality.py    # run_dq_check, validate_row_counts, ...
│       │   ├── lineage.py         # get_data_dictionary, get_lineage, ...
│       │   └── viz.py             # generate_viz, deploy_github_pages, ...
│       └── utils/
│           ├── delta.py           # Delta log checkpoint replay
│           ├── onelake.py         # OneLake REST client
│           └── fabric_api.py      # Fabric API REST client
└── tests/
```

## License

MIT
