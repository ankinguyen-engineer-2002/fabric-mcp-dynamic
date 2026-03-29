"""Azure CLI token management for Fabric MCP Dynamic."""

from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass


@dataclass
class TokenCache:
    token: str = ""
    expires_on: float = 0.0
    resource: str = ""


# Separate caches for different resource scopes
_cache: dict[str, TokenCache] = {}

# Token resources
STORAGE_RESOURCE = "https://storage.azure.com/"
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"


def get_az_token(resource: str = STORAGE_RESOURCE) -> str:
    """Get Azure CLI access token, cached until 2 min before expiry."""
    cached = _cache.get(resource)
    if cached and cached.token and time.time() < (cached.expires_on - 120):
        return cached.token

    result = subprocess.run(
        [
            "az", "account", "get-access-token",
            "--resource", resource,
            "--query", "accessToken",
            "-o", "tsv",
        ],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"az login required. Run: az login\n{result.stderr.strip()}"
        )

    token = result.stdout.strip()
    if not token:
        raise RuntimeError("Empty token returned from az cli")

    # Get expiry
    exp_result = subprocess.run(
        [
            "az", "account", "get-access-token",
            "--resource", resource,
            "--query", "expiresOn",
            "-o", "tsv",
        ],
        capture_output=True, text=True, timeout=30,
    )
    try:
        from datetime import datetime
        expires_on = datetime.fromisoformat(
            exp_result.stdout.strip()
        ).timestamp()
    except Exception:
        expires_on = time.time() + 3000  # fallback ~50min

    _cache[resource] = TokenCache(
        token=token, expires_on=expires_on, resource=resource,
    )
    return token


def check_az_login() -> dict:
    """Verify Azure CLI login status."""
    result = subprocess.run(
        ["az", "account", "show", "--query", "{name:name,state:state}", "-o", "json"],
        capture_output=True, text=True, timeout=15,
    )
    if result.returncode != 0:
        return {"logged_in": False, "error": "Not logged in. Run: az login"}
    return {"logged_in": True, **json.loads(result.stdout)}
