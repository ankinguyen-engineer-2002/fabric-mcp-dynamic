"""Environment tools — health check, switch env, get/set config."""

from __future__ import annotations

from typing import Annotated, Any

from fastmcp.exceptions import ToolError

from fabric_mcp_dynamic.server import mcp
from fabric_mcp_dynamic.auth import check_az_login, get_az_token, STORAGE_RESOURCE, FABRIC_RESOURCE
from fabric_mcp_dynamic.config import get_config, set_config


@mcp.tool
async def health_check() -> dict[str, Any]:
    """Verify Azure CLI login, token validity, and workspace configuration."""
    az_status = check_az_login()

    result: dict[str, Any] = {"az_cli": az_status}

    # Check tokens
    for label, resource in [("storage", STORAGE_RESOURCE), ("fabric_api", FABRIC_RESOURCE)]:
        try:
            get_az_token(resource)
            result[f"{label}_token"] = "valid"
        except Exception as e:
            result[f"{label}_token"] = f"error: {e}"

    # Check config
    cfg = get_config()
    result["config"] = {
        "workspace_id": cfg.workspace_id or "(not set)",
        "lakehouse_id": cfg.lakehouse_id or "(not set)",
        "warehouse_id": cfg.warehouse_id or "(not set)",
        "env": cfg.env,
    }

    config_ok = all([cfg.workspace_id, cfg.lakehouse_id])
    result["ready"] = az_status.get("logged_in", False) and config_ok

    return result


@mcp.tool
async def switch_env(
    env: Annotated[str, "Target environment: 'dev' or 'prod'"],
    workspace_id: Annotated[str | None, "Override workspace GUID for this env"] = None,
    lakehouse_id: Annotated[str | None, "Override lakehouse GUID for this env"] = None,
    warehouse_id: Annotated[str | None, "Override warehouse GUID for this env"] = None,
) -> dict[str, str]:
    """Switch between dev and prod environments. Optionally override IDs."""
    updates: dict[str, str] = {"env": env}
    if workspace_id:
        updates["workspace_id"] = workspace_id
    if lakehouse_id:
        updates["lakehouse_id"] = lakehouse_id
    if warehouse_id:
        updates["warehouse_id"] = warehouse_id

    cfg = set_config(**updates)
    return {
        "env": cfg.env,
        "workspace_id": cfg.workspace_id,
        "lakehouse_id": cfg.lakehouse_id,
        "warehouse_id": cfg.warehouse_id,
    }


@mcp.tool
async def get_current_config() -> dict[str, str]:
    """Get current Fabric MCP configuration."""
    cfg = get_config()
    return {
        "workspace_id": cfg.workspace_id,
        "lakehouse_id": cfg.lakehouse_id,
        "warehouse_id": cfg.warehouse_id,
        "env": cfg.env,
        "onelake_endpoint": cfg.onelake_endpoint,
        "fabric_api": cfg.fabric_api,
    }


@mcp.tool
async def set_current_config(
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
    warehouse_id: Annotated[str | None, "Warehouse GUID"] = None,
    env: Annotated[str | None, "Environment name"] = None,
) -> dict[str, str]:
    """Update Fabric MCP configuration at runtime."""
    updates = {}
    if workspace_id:
        updates["workspace_id"] = workspace_id
    if lakehouse_id:
        updates["lakehouse_id"] = lakehouse_id
    if warehouse_id:
        updates["warehouse_id"] = warehouse_id
    if env:
        updates["env"] = env

    if not updates:
        raise ToolError("No config values provided to update")

    cfg = set_config(**updates)
    return {
        "workspace_id": cfg.workspace_id,
        "lakehouse_id": cfg.lakehouse_id,
        "warehouse_id": cfg.warehouse_id,
        "env": cfg.env,
    }
