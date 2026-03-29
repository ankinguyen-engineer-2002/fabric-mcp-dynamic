"""Fabric REST API client — workspace items, pipelines, notebooks."""

from __future__ import annotations

import base64
import json
from typing import Any

import httpx

from fabric_mcp_dynamic.auth import get_az_token, FABRIC_RESOURCE
from fabric_mcp_dynamic.config import get_config


async def _headers() -> dict[str, str]:
    token = get_az_token(FABRIC_RESOURCE)
    return {"Authorization": f"Bearer {token}"}


async def list_workspace_items(
    client: httpx.AsyncClient,
    workspace_id: str | None = None,
) -> list[dict[str, Any]]:
    """List all items in a Fabric workspace."""
    ws = workspace_id or get_config().workspace_id
    cfg = get_config()
    url = f"{cfg.fabric_api}/v1/workspaces/{ws}/items"
    headers = await _headers()
    items: list[dict] = []
    while url:
        resp = await client.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        items.extend(data.get("value", []))
        url = data.get("continuationUri")
    return items


async def get_item_definition(
    client: httpx.AsyncClient,
    item_id: str,
    item_type: str = "DataPipeline",
    workspace_id: str | None = None,
) -> dict[str, Any]:
    """Get item definition (pipeline JSON, notebook content, etc.)."""
    ws = workspace_id or get_config().workspace_id
    cfg = get_config()
    url = f"{cfg.fabric_api}/v1/workspaces/{ws}/{item_type}s/{item_id}/getDefinition"
    headers = await _headers()
    resp = await client.post(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def decode_pipeline_payload(definition: dict) -> dict[str, Any]:
    """Decode base64 pipeline definition → JSON activities."""
    parts = definition.get("definition", {}).get("parts", [])
    for part in parts:
        if part.get("path", "").endswith("pipeline-content.json"):
            payload = part.get("payload", "")
            decoded = base64.b64decode(payload).decode("utf-8")
            return json.loads(decoded)
    return {}


async def run_pipeline(
    client: httpx.AsyncClient,
    pipeline_id: str,
    workspace_id: str | None = None,
) -> dict[str, Any]:
    """Trigger a pipeline run. Returns run ID."""
    ws = workspace_id or get_config().workspace_id
    cfg = get_config()
    url = f"{cfg.fabric_api}/v1/workspaces/{ws}/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
    headers = await _headers()
    resp = await client.post(url, headers=headers)
    resp.raise_for_status()
    location = resp.headers.get("Location", "")
    return {"status": "accepted", "location": location}


async def get_pipeline_run_status(
    client: httpx.AsyncClient,
    pipeline_id: str,
    run_id: str,
    workspace_id: str | None = None,
) -> dict[str, Any]:
    """Get pipeline run status."""
    ws = workspace_id or get_config().workspace_id
    cfg = get_config()
    url = f"{cfg.fabric_api}/v1/workspaces/{ws}/items/{pipeline_id}/jobs/instances/{run_id}"
    headers = await _headers()
    resp = await client.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


async def get_pipeline_run_history(
    client: httpx.AsyncClient,
    pipeline_id: str,
    workspace_id: str | None = None,
    top: int = 10,
) -> list[dict[str, Any]]:
    """Get recent pipeline run history."""
    ws = workspace_id or get_config().workspace_id
    cfg = get_config()
    url = f"{cfg.fabric_api}/v1/workspaces/{ws}/items/{pipeline_id}/jobs/instances?top={top}"
    headers = await _headers()
    resp = await client.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("value", [])


async def cancel_pipeline_run(
    client: httpx.AsyncClient,
    pipeline_id: str,
    run_id: str,
    workspace_id: str | None = None,
) -> dict[str, Any]:
    """Cancel a running pipeline."""
    ws = workspace_id or get_config().workspace_id
    cfg = get_config()
    url = f"{cfg.fabric_api}/v1/workspaces/{ws}/items/{pipeline_id}/jobs/instances/{run_id}/cancel"
    headers = await _headers()
    resp = await client.post(url, headers=headers)
    resp.raise_for_status()
    return {"status": "cancelled", "run_id": run_id}
