"""Delta Lake log replay — checkpoint + JSON log parsing via OneLake."""

from __future__ import annotations

import io
import json
import re
from typing import Any

import httpx
import pyarrow.parquet as pq
import pandas as pd

from fabric_mcp_dynamic.utils.onelake import list_files, download_file


async def _find_delta_log_files(
    client: httpx.AsyncClient,
    table_path: str,
    workspace_id: str | None = None,
    lakehouse_id: str | None = None,
) -> tuple[str | None, list[str]]:
    """Find latest checkpoint and subsequent JSON logs in _delta_log/."""
    log_path = f"{table_path}/_delta_log"
    files = await list_files(client, log_path, workspace_id, lakehouse_id)

    checkpoint_file: str | None = None
    checkpoint_version: int = -1
    json_logs: list[tuple[int, str]] = []

    for f in files:
        name = f.get("name", "").split("/")[-1]
        if name.endswith(".checkpoint.parquet"):
            match = re.match(r"(\d+)\.checkpoint\.parquet", name)
            if match:
                ver = int(match.group(1))
                if ver > checkpoint_version:
                    checkpoint_version = ver
                    checkpoint_file = f"{log_path}/{name}"
        elif name.endswith(".json"):
            match = re.match(r"(\d+)\.json", name)
            if match:
                json_logs.append((int(match.group(1)), f"{log_path}/{name}"))

    # Only JSON logs AFTER checkpoint
    subsequent = sorted(
        [(v, p) for v, p in json_logs if v > checkpoint_version],
        key=lambda x: x[0],
    )
    return checkpoint_file, [p for _, p in subsequent]


async def _replay_to_active_files(
    client: httpx.AsyncClient,
    checkpoint_path: str | None,
    json_log_paths: list[str],
    workspace_id: str | None = None,
    lakehouse_id: str | None = None,
) -> set[str]:
    """Replay checkpoint + logs to determine active parquet files."""
    active_files: set[str] = set()

    # Parse checkpoint
    if checkpoint_path:
        data = await download_file(client, checkpoint_path, workspace_id, lakehouse_id)
        table = pq.read_table(io.BytesIO(data))
        df = table.to_pandas()
        for _, row in df.iterrows():
            add = row.get("add")
            if add is not None and isinstance(add, dict) and add.get("path"):
                active_files.add(add["path"])

    # Apply subsequent JSON logs
    for log_path in json_log_paths:
        data = await download_file(client, log_path, workspace_id, lakehouse_id)
        for line in data.decode("utf-8").strip().split("\n"):
            if not line.strip():
                continue
            entry = json.loads(line)
            if "add" in entry:
                active_files.add(entry["add"]["path"])
            if "remove" in entry:
                active_files.discard(entry["remove"]["path"])

    return active_files


async def read_delta_table(
    client: httpx.AsyncClient,
    table_path: str,
    workspace_id: str | None = None,
    lakehouse_id: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Read a Delta table via checkpoint replay → download active parquets → merge."""
    checkpoint, json_logs = await _find_delta_log_files(
        client, table_path, workspace_id, lakehouse_id,
    )
    active_files = await _replay_to_active_files(
        client, checkpoint, json_logs, workspace_id, lakehouse_id,
    )

    if not active_files:
        return pd.DataFrame()

    dfs: list[pd.DataFrame] = []
    for file_path in sorted(active_files):
        full_path = f"{table_path}/{file_path}"
        data = await download_file(client, full_path, workspace_id, lakehouse_id)
        df = pq.read_table(io.BytesIO(data)).to_pandas()
        dfs.append(df)

    result = pd.concat(dfs, ignore_index=True)
    if limit:
        result = result.head(limit)
    return result


async def get_table_schema(
    client: httpx.AsyncClient,
    table_path: str,
    workspace_id: str | None = None,
    lakehouse_id: str | None = None,
) -> list[dict[str, str]]:
    """Get column names and types from the first active parquet file."""
    checkpoint, json_logs = await _find_delta_log_files(
        client, table_path, workspace_id, lakehouse_id,
    )
    active_files = await _replay_to_active_files(
        client, checkpoint, json_logs, workspace_id, lakehouse_id,
    )

    if not active_files:
        return []

    first_file = sorted(active_files)[0]
    data = await download_file(
        client, f"{table_path}/{first_file}", workspace_id, lakehouse_id,
    )
    schema = pq.read_schema(io.BytesIO(data))
    return [{"name": f.name, "type": str(f.type)} for f in schema]
