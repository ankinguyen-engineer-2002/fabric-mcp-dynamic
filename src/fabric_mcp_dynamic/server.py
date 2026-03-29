"""MCP server entry point — FastMCP instance with shared httpx client."""

from __future__ import annotations

import httpx
from fastmcp import FastMCP
from fastmcp.server.lifespan import lifespan


@lifespan
async def app_lifespan(server):
    """Create shared httpx client, auto-close on shutdown."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        yield {"http_client": client}


mcp = FastMCP(
    name="fabric-dynamic",
    instructions=(
        "Fabric MCP Dynamic — Custom MCP server for Microsoft Fabric. "
        "Provides 27 tools: workspace discovery, Delta table reads (OneLake checkpoint replay), "
        "pipeline orchestration, data quality checks, lineage/metadata, "
        "D3.js visualization, and environment management. "
        "Auth: uses 'az login' tokens (no Service Principal needed). "
        "Call health_check first to verify connectivity."
    ),
    lifespan=app_lifespan,
)

# Import all tool modules so their @mcp.tool decorators register
from fabric_mcp_dynamic.tools import (  # noqa: E402, F401
    discovery,
    data_ops,
    pipeline,
    data_quality,
    lineage,
    viz,
    environment,
)
