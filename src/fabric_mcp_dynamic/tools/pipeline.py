"""Pipeline tools — trigger, status, history, cancel."""

from __future__ import annotations

from typing import Annotated, Any

from fastmcp import Context
from fastmcp.exceptions import ToolError

from fabric_mcp_dynamic.server import mcp
from fabric_mcp_dynamic.utils import fabric_api


@mcp.tool
async def trigger_pipeline(
    ctx: Context,
    pipeline_id: Annotated[str, "Pipeline item ID (GUID)"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
) -> dict[str, Any]:
    """Trigger a pipeline run. Returns the run location URL for status tracking."""
    client = ctx.lifespan_context["http_client"]
    try:
        return await fabric_api.run_pipeline(client, pipeline_id, workspace_id)
    except Exception as e:
        raise ToolError(f"Failed to trigger pipeline: {e}")


@mcp.tool
async def get_pipeline_status(
    ctx: Context,
    pipeline_id: Annotated[str, "Pipeline item ID (GUID)"],
    run_id: Annotated[str, "Pipeline run ID (GUID)"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
) -> dict[str, Any]:
    """Check the status of a specific pipeline run."""
    client = ctx.lifespan_context["http_client"]
    try:
        return await fabric_api.get_pipeline_run_status(client, pipeline_id, run_id, workspace_id)
    except Exception as e:
        raise ToolError(f"Failed to get pipeline status: {e}")


@mcp.tool
async def get_pipeline_history(
    ctx: Context,
    pipeline_id: Annotated[str, "Pipeline item ID (GUID)"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    top: Annotated[int, "Number of recent runs to return"] = 10,
) -> list[dict[str, Any]]:
    """Get recent pipeline run history."""
    client = ctx.lifespan_context["http_client"]
    try:
        return await fabric_api.get_pipeline_run_history(client, pipeline_id, workspace_id, top)
    except Exception as e:
        raise ToolError(f"Failed to get pipeline history: {e}")


@mcp.tool
async def cancel_pipeline(
    ctx: Context,
    pipeline_id: Annotated[str, "Pipeline item ID (GUID)"],
    run_id: Annotated[str, "Pipeline run ID to cancel"],
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
) -> dict[str, Any]:
    """Cancel a running pipeline."""
    client = ctx.lifespan_context["http_client"]
    try:
        return await fabric_api.cancel_pipeline_run(client, pipeline_id, run_id, workspace_id)
    except Exception as e:
        raise ToolError(f"Failed to cancel pipeline: {e}")
