"""Environment configuration for Fabric MCP Dynamic."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class FabricConfig:
    workspace_id: str = ""
    lakehouse_id: str = ""
    warehouse_id: str = ""
    env: str = "dev"

    # Endpoints (rarely changed)
    onelake_endpoint: str = "https://onelake.dfs.fabric.microsoft.com"
    fabric_api: str = "https://api.fabric.microsoft.com"

    @classmethod
    def from_env(cls) -> FabricConfig:
        return cls(
            workspace_id=os.environ.get("FABRIC_WORKSPACE_ID", ""),
            lakehouse_id=os.environ.get("FABRIC_LAKEHOUSE_ID", ""),
            warehouse_id=os.environ.get("FABRIC_WAREHOUSE_ID", ""),
            env=os.environ.get("FABRIC_ENV", "dev"),
        )


# Singleton — loaded once at startup
_config: FabricConfig | None = None


def get_config() -> FabricConfig:
    global _config
    if _config is None:
        _config = FabricConfig.from_env()
    return _config


def set_config(**kwargs: str) -> FabricConfig:
    global _config
    cfg = get_config()
    for k, v in kwargs.items():
        if hasattr(cfg, k):
            setattr(cfg, k, v)
    return cfg
