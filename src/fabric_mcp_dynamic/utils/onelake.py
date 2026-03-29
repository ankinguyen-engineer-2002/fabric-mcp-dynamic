"""OneLake REST client — file listing, blob download."""

from __future__ import annotations

from typing import Any

import httpx

from fabric_mcp_dynamic.auth import get_az_token, STORAGE_RESOURCE
from fabric_mcp_dynamic.config import get_config


def _base_url(workspace_id: str | None = None, lakehouse_id: str | None = None) -> str:
    cfg = get_config()
    ws = workspace_id or cfg.workspace_id
    lh = lakehouse_id or cfg.lakehouse_id
    return f"{cfg.onelake_endpoint}/{ws}/{lh}"


async def _headers() -> dict[str, str]:
    token = get_az_token(STORAGE_RESOURCE)
    return {"Authorization": f"Bearer {token}"}


async def list_files(
    client: httpx.AsyncClient,
    path: str,
    workspace_id: str | None = None,
    lakehouse_id: str | None = None,
) -> list[dict[str, Any]]:
    """List files/directories at a OneLake path."""
    base = _base_url(workspace_id, lakehouse_id)
    url = f"{base}/{path}?resource=filesystem&recursive=true"
    headers = await _headers()
    resp = await client.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("paths", [])


async def download_file(
    client: httpx.AsyncClient,
    path: str,
    workspace_id: str | None = None,
    lakehouse_id: str | None = None,
) -> bytes:
    """Download a file from OneLake as bytes."""
    base = _base_url(workspace_id, lakehouse_id)
    url = f"{base}/{path}"
    headers = await _headers()
    resp = await client.get(url, headers=headers)
    resp.raise_for_status()
    return resp.content
