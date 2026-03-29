"""Data Quality tools — DQ checks, freshness, row count validation."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Annotated, Any

import pandas as pd
from fastmcp import Context
from fastmcp.exceptions import ToolError

from fabric_mcp_dynamic.server import mcp
from fabric_mcp_dynamic.utils import delta


@mcp.tool
async def run_dq_check(
    ctx: Context,
    table_name: Annotated[str, "Table to validate (e.g. 'dbo/utl_pipeline_metadata')"],
    rules: Annotated[list[dict[str, str]] | None, "List of DQ rules: [{'column': 'x', 'check': 'not_null|unique|range', 'params': '...'}]. If omitted, runs default checks."] = None,
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> dict[str, Any]:
    """Run data quality checks on a Delta table. Returns pass/fail per rule."""
    client = ctx.lifespan_context["http_client"]
    table_path = f"Tables/{table_name.replace('.', '/')}" if "." in table_name else f"Tables/{table_name}"

    try:
        df = await delta.read_delta_table(client, table_path, workspace_id, lakehouse_id)
    except Exception as e:
        raise ToolError(f"Failed to read table for DQ: {e}")

    if df.empty:
        return {"table": table_name, "status": "empty", "checks": []}

    results = []

    if rules:
        for rule in rules:
            col = rule.get("column", "")
            check = rule.get("check", "")
            result = _run_single_check(df, col, check, rule.get("params", ""))
            results.append(result)
    else:
        # Default checks: not_null on all columns, row count > 0
        results.append({"check": "row_count", "column": "*", "passed": len(df) > 0, "value": len(df)})
        for col in df.columns:
            null_count = int(df[col].isna().sum())
            results.append({
                "check": "null_count", "column": col,
                "passed": null_count == 0, "value": null_count,
            })

    passed = sum(1 for r in results if r["passed"])
    return {
        "table": table_name,
        "total_checks": len(results),
        "passed": passed,
        "failed": len(results) - passed,
        "status": "pass" if passed == len(results) else "fail",
        "checks": results,
    }


def _run_single_check(df: pd.DataFrame, column: str, check: str, params: str) -> dict[str, Any]:
    """Run a single DQ check."""
    if column and column not in df.columns:
        return {"check": check, "column": column, "passed": False, "error": "column not found"}

    if check == "not_null":
        null_count = int(df[column].isna().sum())
        return {"check": check, "column": column, "passed": null_count == 0, "value": null_count}
    elif check == "unique":
        dup_count = int(df[column].duplicated().sum())
        return {"check": check, "column": column, "passed": dup_count == 0, "value": dup_count}
    elif check == "row_count":
        expected = int(params) if params else 0
        actual = len(df)
        return {"check": check, "column": "*", "passed": actual == expected, "expected": expected, "actual": actual}
    elif check == "min_rows":
        minimum = int(params) if params else 1
        return {"check": check, "column": "*", "passed": len(df) >= minimum, "value": len(df), "minimum": minimum}
    else:
        return {"check": check, "column": column, "passed": False, "error": f"unknown check type: {check}"}


@mcp.tool
async def get_dq_results(
    ctx: Context,
    results_table: Annotated[str, "DQ results table name"] = "dbo/utl_dq_results",
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
    limit: Annotated[int, "Max rows"] = 50,
) -> list[dict[str, Any]]:
    """Read historical DQ results from a Delta table."""
    client = ctx.lifespan_context["http_client"]
    table_path = f"Tables/{results_table.replace('.', '/')}" if "." in results_table else f"Tables/{results_table}"

    try:
        df = await delta.read_delta_table(client, table_path, workspace_id, lakehouse_id, limit=limit)
        return df.to_dict(orient="records") if not df.empty else []
    except Exception as e:
        raise ToolError(f"Failed to read DQ results: {e}")


@mcp.tool
async def validate_row_counts(
    ctx: Context,
    tables: Annotated[list[str], "List of table names to validate row counts"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> dict[str, Any]:
    """Cross-validate row counts across multiple tables. Useful for layer-to-layer validation."""
    client = ctx.lifespan_context["http_client"]
    results = []

    for table_name in tables:
        table_path = f"Tables/{table_name.replace('.', '/')}" if "." in table_name else f"Tables/{table_name}"
        try:
            df = await delta.read_delta_table(client, table_path, workspace_id, lakehouse_id)
            results.append({"table": table_name, "row_count": len(df), "status": "ok"})
        except Exception as e:
            results.append({"table": table_name, "row_count": -1, "status": f"error: {e}"})

    return {
        "tables_checked": len(results),
        "total_rows": sum(r["row_count"] for r in results if r["row_count"] >= 0),
        "errors": sum(1 for r in results if r["status"] != "ok"),
        "details": results,
    }


@mcp.tool
async def check_freshness(
    ctx: Context,
    table_name: Annotated[str, "Table name to check"],
    timestamp_column: Annotated[str, "Column containing last-updated timestamp"] = "ts_updated",
    max_age_hours: Annotated[int, "Max acceptable age in hours"] = 24,
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> dict[str, Any]:
    """Check if a table's data is fresh (not stale). Compares latest timestamp to now."""
    client = ctx.lifespan_context["http_client"]
    table_path = f"Tables/{table_name.replace('.', '/')}" if "." in table_name else f"Tables/{table_name}"

    try:
        df = await delta.read_delta_table(client, table_path, workspace_id, lakehouse_id)
    except Exception as e:
        raise ToolError(f"Failed to read table: {e}")

    if df.empty:
        return {"table": table_name, "fresh": False, "reason": "table is empty"}

    if timestamp_column not in df.columns:
        return {"table": table_name, "fresh": False, "reason": f"column '{timestamp_column}' not found"}

    try:
        latest = pd.to_datetime(df[timestamp_column]).max()
        now = pd.Timestamp.now(tz="UTC")
        if latest.tzinfo is None:
            latest = latest.tz_localize("UTC")
        age_hours = (now - latest).total_seconds() / 3600
        return {
            "table": table_name,
            "fresh": age_hours <= max_age_hours,
            "latest_timestamp": str(latest),
            "age_hours": round(age_hours, 1),
            "max_age_hours": max_age_hours,
        }
    except Exception as e:
        return {"table": table_name, "fresh": False, "reason": f"Failed to parse timestamps: {e}"}
