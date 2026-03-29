"""Data operations — query, read, schema, preview Delta tables."""

from __future__ import annotations

from typing import Annotated, Any

from fastmcp import Context
from fastmcp.exceptions import ToolError

from fabric_mcp_dynamic.server import mcp
from fabric_mcp_dynamic.utils import delta


@mcp.tool
async def read_delta_table(
    ctx: Context,
    table_name: Annotated[str, "Table name (e.g. 'dbo/my_table' or 'dbo.my_table')"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
    limit: Annotated[int, "Max rows to return"] = 100,
) -> list[dict[str, Any]]:
    """Read a Delta table directly from OneLake via checkpoint replay. Returns rows as dicts."""
    client = ctx.lifespan_context["http_client"]
    table_path = f"Tables/{table_name.replace('.', '/')}" if "." in table_name else f"Tables/{table_name}"

    try:
        df = await delta.read_delta_table(client, table_path, workspace_id, lakehouse_id, limit=limit)
    except Exception as e:
        raise ToolError(f"Failed to read table: {e}")

    return df.to_dict(orient="records") if not df.empty else []


@mcp.tool
async def get_table_schema(
    ctx: Context,
    table_name: Annotated[str, "Table name (e.g. 'dbo/my_table' or 'dbo.my_table')"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> list[dict[str, str]]:
    """Get column names and types for a Delta table."""
    client = ctx.lifespan_context["http_client"]
    table_path = f"Tables/{table_name.replace('.', '/')}" if "." in table_name else f"Tables/{table_name}"

    try:
        return await delta.get_table_schema(client, table_path, workspace_id, lakehouse_id)
    except Exception as e:
        raise ToolError(f"Failed to get schema: {e}")


@mcp.tool
async def get_row_count(
    ctx: Context,
    table_name: Annotated[str, "Table name"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> dict[str, Any]:
    """Get row count for a Delta table."""
    client = ctx.lifespan_context["http_client"]
    table_path = f"Tables/{table_name.replace('.', '/')}" if "." in table_name else f"Tables/{table_name}"

    try:
        df = await delta.read_delta_table(client, table_path, workspace_id, lakehouse_id)
        return {"table": table_name, "row_count": len(df)}
    except Exception as e:
        raise ToolError(f"Failed to count rows: {e}")


@mcp.tool
async def preview_table(
    ctx: Context,
    table_name: Annotated[str, "Table name"],
    rows: Annotated[int, "Number of rows to preview"] = 5,
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> dict[str, Any]:
    """Preview top N rows from a Delta table with schema info."""
    client = ctx.lifespan_context["http_client"]
    table_path = f"Tables/{table_name.replace('.', '/')}" if "." in table_name else f"Tables/{table_name}"

    try:
        schema = await delta.get_table_schema(client, table_path, workspace_id, lakehouse_id)
        df = await delta.read_delta_table(client, table_path, workspace_id, lakehouse_id, limit=rows)
        return {
            "table": table_name,
            "columns": schema,
            "total_columns": len(schema),
            "preview": df.to_dict(orient="records") if not df.empty else [],
        }
    except Exception as e:
        raise ToolError(f"Failed to preview table: {e}")


@mcp.tool
async def query_lakehouse(
    ctx: Context,
    sql: Annotated[str, "SQL SELECT query to execute on Lakehouse SQL Endpoint"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
) -> dict[str, Any]:
    """Execute a read-only SQL query on the Lakehouse SQL Analytics Endpoint.
    Note: Requires SQL endpoint to be enabled on the lakehouse."""
    # SQL endpoint queries go through Fabric API, not OneLake
    # This is a placeholder — full implementation requires TDS/ODBC connection
    raise ToolError(
        "query_lakehouse requires a SQL endpoint connection (TDS/ODBC). "
        "Use read_delta_table for direct OneLake reads, or configure "
        "the mssql MCP server for SQL queries."
    )


@mcp.tool
async def query_warehouse(
    ctx: Context,
    sql: Annotated[str, "T-SQL query to execute on Fabric Warehouse"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
) -> dict[str, Any]:
    """Execute a T-SQL query on a Fabric Warehouse.
    Note: Requires Warehouse SQL connection."""
    raise ToolError(
        "query_warehouse requires a Warehouse SQL connection (TDS/ODBC). "
        "Configure the mssql MCP server for Warehouse queries."
    )
