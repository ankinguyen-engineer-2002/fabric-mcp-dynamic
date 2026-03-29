"""Discovery tools — scan workspace, get metadata, pipeline defs, notebooks."""

from __future__ import annotations

import re
from collections import Counter
from typing import Annotated, Any

from fastmcp import Context
from fastmcp.exceptions import ToolError

from fabric_mcp_dynamic.server import mcp
from fabric_mcp_dynamic.config import get_config
from fabric_mcp_dynamic.utils import fabric_api, delta


@mcp.tool
async def scan_workspace(
    ctx: Context,
    workspace_id: Annotated[str | None, "Fabric workspace GUID. Uses default if omitted."] = None,
) -> dict[str, Any]:
    """Scan all items in a Fabric workspace. Returns items grouped by type with counts."""
    client = ctx.lifespan_context["http_client"]
    try:
        items = await fabric_api.list_workspace_items(client, workspace_id)
    except Exception as e:
        raise ToolError(f"Failed to scan workspace: {e}")

    by_type: dict[str, list[str]] = {}
    for item in items:
        t = item.get("type", "Unknown")
        by_type.setdefault(t, []).append(item.get("displayName", ""))

    return {
        "total_items": len(items),
        "by_type": {t: {"count": len(names), "items": sorted(names)} for t, names in sorted(by_type.items())},
    }


@mcp.tool
async def get_metadata(
    ctx: Context,
    table_name: Annotated[str, "Delta table name (e.g. 'dbo.utl_pipeline_metadata')"] = "dbo.utl_pipeline_metadata",
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> list[dict[str, Any]]:
    """Read pipeline metadata from a Delta table via OneLake checkpoint replay.
    Returns parsed rows with layer, table_name, status, rows_loaded, runtime, etc."""
    client = ctx.lifespan_context["http_client"]
    table_path = f"Tables/{table_name.replace('.', '/')}" if "." in table_name else f"Tables/{table_name}"

    try:
        df = await delta.read_delta_table(client, table_path, workspace_id, lakehouse_id)
    except Exception as e:
        raise ToolError(f"Failed to read Delta table: {e}")

    if df.empty:
        return []

    def extract_runtime(notes: Any) -> str:
        m = re.search(r"in (\d+)s", str(notes))
        if m:
            s = int(m.group(1))
            return f"{s // 60}m{s % 60:02d}s" if s >= 60 else f"{s}s"
        return "n/a"

    layer_order = {"REF": 0, "BRZ": 1, "SLV": 2, "GLD": 3}
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "layer": str(r.get("layer", "")),
            "table_name": str(r.get("table_name", "")),
            "frequency": str(r.get("frequency", "Daily")),
            "execution_order": int(r.get("execution_order", 0)) if r.get("execution_order") is not None else 0,
            "load_type": str(r.get("load_type", "overwrite")),
            "rows_loaded": int(r.get("rows_loaded", 0)) if r.get("rows_loaded") is not None else 0,
            "status": str(r.get("status", "")),
            "notebook_name": str(r.get("notebook_name", ""))[:12],
            "runtime": extract_runtime(r.get("pipeline_notes", "")),
        })
    rows.sort(key=lambda x: (layer_order.get(x["layer"], 99), x["execution_order"], x["table_name"]))
    return rows


@mcp.tool
async def get_pipeline_def(
    ctx: Context,
    pipeline_id: Annotated[str, "Pipeline item ID (GUID)"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
) -> dict[str, Any]:
    """Get and decode a pipeline definition. Returns activities, dependencies, and parameters."""
    client = ctx.lifespan_context["http_client"]
    try:
        definition = await fabric_api.get_item_definition(
            client, pipeline_id, "DataPipeline", workspace_id,
        )
        decoded = fabric_api.decode_pipeline_payload(definition)
        return decoded if decoded else {"raw": definition}
    except Exception as e:
        raise ToolError(f"Failed to get pipeline definition: {e}")


@mcp.tool
async def get_notebook_list(
    ctx: Context,
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    filter_layer: Annotated[str | None, "Filter by layer prefix (e.g. 'brz', 'slv')"] = None,
) -> list[dict[str, str]]:
    """List all notebooks in a workspace, optionally filtered by layer prefix."""
    client = ctx.lifespan_context["http_client"]
    try:
        items = await fabric_api.list_workspace_items(client, workspace_id)
    except Exception as e:
        raise ToolError(f"Failed to list items: {e}")

    notebooks = [
        {"id": i["id"], "name": i["displayName"]}
        for i in items
        if i.get("type") == "Notebook"
    ]

    if filter_layer:
        prefix = filter_layer.lower()
        notebooks = [n for n in notebooks if n["name"].lower().startswith(f"nb_{prefix}")]

    return sorted(notebooks, key=lambda x: x["name"])
