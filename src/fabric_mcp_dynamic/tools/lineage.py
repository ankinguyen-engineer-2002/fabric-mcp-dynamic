"""Metadata & Lineage tools — data dictionary, lineage graph, metadata update."""

from __future__ import annotations

import io
import json
from typing import Annotated, Any

import pyarrow.parquet as pq
from fastmcp import Context
from fastmcp.exceptions import ToolError

from fabric_mcp_dynamic.server import mcp
from fabric_mcp_dynamic.utils import delta, onelake


@mcp.tool
async def get_data_dictionary(
    ctx: Context,
    tables: Annotated[list[str] | None, "Specific tables to include. If omitted, scans all tables in lakehouse."] = None,
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> list[dict[str, Any]]:
    """Generate a data dictionary: table name, layer, columns with types.
    Scans Delta log initial entry or first parquet for each table's schema."""
    client = ctx.lifespan_context["http_client"]

    if tables:
        table_names = tables
    else:
        # List all tables in Tables/ directory
        try:
            files = await onelake.list_files(client, "Tables", workspace_id, lakehouse_id)
            # Extract unique table directory names (Tables/dbo/table_name/)
            seen: set[str] = set()
            table_names = []
            for f in files:
                name = f.get("name", "")
                parts = name.split("/")
                if len(parts) >= 3 and parts[0] == "Tables":
                    table_key = f"{parts[1]}/{parts[2]}"
                    if table_key not in seen:
                        seen.add(table_key)
                        table_names.append(table_key)
        except Exception as e:
            raise ToolError(f"Failed to list tables: {e}")

    dictionary = []
    for table_name in sorted(table_names):
        table_path = f"Tables/{table_name}"
        try:
            schema = await delta.get_table_schema(client, table_path, workspace_id, lakehouse_id)
            # Determine layer from name
            short_name = table_name.split("/")[-1] if "/" in table_name else table_name
            layer = _detect_layer(short_name)
            dictionary.append({
                "table": table_name,
                "short_name": short_name,
                "layer": layer,
                "columns": schema,
                "column_count": len(schema),
            })
        except Exception:
            dictionary.append({
                "table": table_name,
                "short_name": table_name.split("/")[-1],
                "layer": "unknown",
                "columns": [],
                "column_count": 0,
                "error": "failed to read schema",
            })

    return dictionary


def _detect_layer(name: str) -> str:
    """Detect medallion layer from table name prefix."""
    prefixes = {
        "brz_": "BRZ", "ref_": "REF", "slv_": "SLV", "gld_": "GLD",
        "dim_": "DIM", "fact_": "FACT", "stg_": "STG", "utl_": "UTL",
    }
    for prefix, layer in prefixes.items():
        if name.startswith(prefix):
            return layer
    return "unknown"


@mcp.tool
async def get_lineage(
    ctx: Context,
    table_name: Annotated[str, "Lineage table name"] = "dbo/utl_lineage",
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> list[dict[str, Any]]:
    """Read lineage metadata from a Delta table. Returns source→target mappings."""
    client = ctx.lifespan_context["http_client"]
    table_path = f"Tables/{table_name.replace('.', '/')}" if "." in table_name else f"Tables/{table_name}"

    try:
        df = await delta.read_delta_table(client, table_path, workspace_id, lakehouse_id)
        return df.to_dict(orient="records") if not df.empty else []
    except Exception as e:
        raise ToolError(f"Failed to read lineage table: {e}")


@mcp.tool
async def update_metadata(
    ctx: Context,
    table_name: Annotated[str, "Target metadata table"],
    records: Annotated[list[dict[str, Any]], "Records to write as JSON dicts"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> dict[str, Any]:
    """Write metadata records to a Delta table via OneLake (append parquet file).
    Note: This appends a new parquet part file — it does NOT do merge/upsert."""
    import pandas as pd

    client = ctx.lifespan_context["http_client"]
    table_path = f"Tables/{table_name.replace('.', '/')}" if "." in table_name else f"Tables/{table_name}"

    try:
        df = pd.DataFrame(records)
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", index=False)
        buf.seek(0)

        # Upload as a new part file
        import hashlib
        import time
        part_hash = hashlib.md5(str(time.time()).encode()).hexdigest()[:12]
        file_path = f"{table_path}/part-mcp-{part_hash}.snappy.parquet"

        from fabric_mcp_dynamic.utils.onelake import _base_url, _headers
        base = _base_url(workspace_id, lakehouse_id)
        url = f"{base}/{file_path}"
        headers = await _headers()
        headers["Content-Type"] = "application/octet-stream"

        resp = await client.put(url, content=buf.getvalue(), headers=headers)
        resp.raise_for_status()

        return {
            "status": "appended",
            "table": table_name,
            "records_written": len(records),
            "file": file_path,
        }
    except Exception as e:
        raise ToolError(f"Failed to update metadata: {e}")
