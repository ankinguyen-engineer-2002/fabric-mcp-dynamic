"""Lineage discovery tools — workspace-aware pipeline/notebook/table lineage.

Three tools designed for different scenarios:
  1. discover_lineage  — WORKSPACE: full orchestration + table mapping from Fabric API
  2. trace_table       — WORKSPACE: debug a specific table upstream/downstream
  3. scan_local_folder — LOCAL: parse notebook code for source→target table detection

Architecture-portable: auto-detects metadata schema, layer naming, pipeline patterns.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Annotated, Any

from fastmcp import Context
from fastmcp.exceptions import ToolError

from fabric_mcp_dynamic.server import mcp
from fabric_mcp_dynamic.config import get_config
from fabric_mcp_dynamic.utils import fabric_api, delta


# ═══════════════════════════════════════════════════════════════════════════
# Portable helpers — no hardcoded schemas or layer names
# ═══════════════════════════════════════════════════════════════════════════

# Common layer prefixes (extensible — auto-detected layers always take priority)
_KNOWN_LAYER_PREFIXES = {
    "brz_": "BRZ", "bronze_": "BRZ", "raw_": "RAW",
    "ref_": "REF", "lkp_": "LKP", "lookup_": "LKP",
    "slv_": "SLV", "silver_": "SLV", "cleansed_": "SLV",
    "gld_": "GLD", "gold_": "GLD", "curated_": "GLD",
    "dim_": "DIM", "fact_": "FACT",
    "stg_": "STG", "staging_": "STG",
    "utl_": "UTL", "util_": "UTL", "meta_": "META",
}


def _detect_layer(name: str) -> str:
    """Detect medallion layer from a file/table name prefix.
    Handles nb_ prefix (e.g. nb_brz_engine → BRZ)."""
    lower = name.lower()
    # Strip common notebook prefix
    check = lower[3:] if lower.startswith("nb_") else lower
    for prefix, layer in _KNOWN_LAYER_PREFIXES.items():
        if check.startswith(prefix):
            return layer
    return "unknown"


def _safe_int(val: Any) -> int | str:
    """Convert to int safely, return '' if not possible."""
    if val is None or val == "":
        return ""
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return ""


# ═══════════════════════════════════════════════════════════════════════════
# Pipeline activity parser — handles ALL Fabric activity types generically
# ═══════════════════════════════════════════════════════════════════════════

def _parse_activities(activities: list[dict]) -> list[dict[str, Any]]:
    """Recursively parse pipeline activities into a normalized structure.
    Generic: extracts what it can from any activity type."""
    results = []
    for act in activities:
        act_type = act.get("type", "Unknown")
        act_name = act.get("name", "")
        deps = [
            {"activity": d.get("activity", ""), "conditions": d.get("dependencyConditions", [])}
            for d in act.get("dependsOn", [])
        ]
        info: dict[str, Any] = {"name": act_name, "type": act_type, "depends_on": deps}
        tp = act.get("typeProperties", {})

        # --- Notebook activities ---
        if act_type in ("TridentNotebook", "SynapseNotebook", "DatabricksNotebook",
                        "SparkJob", "HDInsightSpark"):
            nb_id = tp.get("notebookId") or tp.get("notebook", {}).get("referenceName", "")
            if isinstance(nb_id, dict):
                info["notebook_id"] = nb_id.get("value", "")
                info["notebook_dynamic"] = True
            else:
                info["notebook_id"] = str(nb_id)
                info["notebook_dynamic"] = False
            params = tp.get("parameters", tp.get("baseParameters", {}))
            if params:
                info["parameters"] = {
                    k: (v.get("value", v) if isinstance(v, dict) else v)
                    for k, v in params.items()
                }

        # --- ForEach ---
        elif act_type == "ForEach":
            info["items_expr"] = tp.get("items", {}).get("value", "")
            info["batch_count"] = tp.get("batchCount", 1)
            info["inner_activities"] = _parse_activities(tp.get("activities", []))

        # --- IfCondition ---
        elif act_type == "IfCondition":
            info["condition"] = tp.get("expression", {}).get("value", "")
            for branch in ("ifTrueActivities", "ifFalseActivities"):
                branch_acts = tp.get(branch, [])
                if branch_acts:
                    info[branch] = _parse_activities(branch_acts)

        # --- Switch ---
        elif act_type == "Switch":
            info["switch_on"] = tp.get("on", {}).get("value", "")
            cases = tp.get("cases", [])
            info["cases"] = []
            for case in cases:
                info["cases"].append({
                    "value": case.get("value", ""),
                    "activities": _parse_activities(case.get("activities", [])),
                })
            default_acts = tp.get("defaultActivities", [])
            if default_acts:
                info["default_activities"] = _parse_activities(default_acts)

        # --- Until / While ---
        elif act_type in ("Until", "While"):
            info["condition"] = tp.get("expression", {}).get("value", "")
            info["inner_activities"] = _parse_activities(tp.get("activities", []))
            info["timeout"] = tp.get("timeout", "")

        # --- Pipeline calls ---
        elif act_type in ("ExecutePipeline", "InvokePipeline"):
            info["called_pipeline"] = tp.get("pipeline", {}).get("referenceName", "")
            info["called_pipeline_id"] = tp.get("pipelineId", "")

        # --- Lookup ---
        elif act_type == "Lookup":
            src = tp.get("source", {})
            info["lookup_query"] = src.get("sqlReaderQuery", "")

        # --- Stored Procedure ---
        elif act_type == "SqlServerStoredProcedure":
            info["stored_procedure"] = tp.get("storedProcedureName", "")
            ls = act.get("linkedService", {})
            info["linked_service"] = ls.get("name", "")

        # --- Dataflow ---
        elif act_type == "RefreshDataflow":
            info["dataflow_id"] = tp.get("dataflowId", "")

        # --- Copy Activity ---
        elif act_type == "Copy":
            info["source_type"] = tp.get("source", {}).get("type", "")
            info["sink_type"] = tp.get("sink", {}).get("type", "")

        # --- Web / Script / other ---
        elif act_type in ("WebActivity", "WebHook"):
            info["url"] = tp.get("url", "")
            info["method"] = tp.get("method", "")

        # --- Generic fallback: capture typeProperties keys for unknown types ---
        else:
            if tp:
                info["type_properties_keys"] = list(tp.keys())

        results.append(info)
    return results


def _collect_all_notebook_ids(activities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Recursively collect ALL notebook references from parsed activities,
    including nested ForEach, IfCondition, Switch, Until branches."""
    refs = []
    for act in activities:
        if "notebook_id" in act:
            refs.append({
                "notebook_id": act["notebook_id"],
                "dynamic": act.get("notebook_dynamic", False),
                "activity": act["name"],
            })
        # Recurse into all nested structures
        for key in ("inner_activities", "ifTrueActivities", "ifFalseActivities", "default_activities"):
            refs.extend(_collect_all_notebook_ids(act.get(key, [])))
        # Switch cases
        for case in act.get("cases", []):
            refs.extend(_collect_all_notebook_ids(case.get("activities", [])))
    return refs


def _extract_lookup_filters(activities: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Extract filter conditions from Lookup SQL queries.
    Generic: captures any column = 'value' or column = N patterns."""
    filters = []
    for act in activities:
        if act.get("type") == "Lookup" and act.get("lookup_query"):
            q = act["lookup_query"]
            entry: dict[str, str] = {"activity": act["name"], "raw_query": q}
            # Extract all WHERE conditions generically
            conditions = re.findall(r"(\w+)\s*=\s*(?:'([^']+)'|(\d+))", q, re.IGNORECASE)
            for col, str_val, num_val in conditions:
                col_lower = col.lower()
                val = str_val or num_val
                # Skip common non-filter columns
                if col_lower in ("current_timestamp", "getdate", "sysdatetime"):
                    continue
                entry[col_lower] = val
            if len(entry) > 2:  # has at least one real filter
                filters.append(entry)
    return filters


# ═══════════════════════════════════════════════════════════════════════════
# Metadata table auto-detection
# ═══════════════════════════════════════════════════════════════════════════

# Column name aliases — maps semantic role → possible column names
_COL_ALIASES = {
    "table_name":       ["table_name", "target_table", "table", "object_name", "entity_name", "tbl_name"],
    "notebook_name":    ["notebook_name", "notebook_id", "notebook", "nb_name", "nb_id", "process_name"],
    "layer":            ["layer", "medallion_layer", "zone", "tier", "stage"],
    "execution_order":  ["execution_order", "exec_order", "run_order", "priority", "sequence", "order_no"],
    "load_type":        ["load_type", "load_strategy", "write_mode", "mode", "strategy"],
    "frequency":        ["frequency", "schedule", "refresh_freq", "cadence", "run_frequency"],
    "is_active":        ["is_active", "active", "enabled", "is_enabled", "status"],
}


def _resolve_columns(df_columns: list[str]) -> dict[str, str | None]:
    """Auto-detect which DataFrame columns match our semantic roles.
    Returns mapping: semantic_role → actual_column_name (or None)."""
    col_set = {c.lower(): c for c in df_columns}
    resolved = {}
    for role, aliases in _COL_ALIASES.items():
        resolved[role] = None
        for alias in aliases:
            if alias.lower() in col_set:
                resolved[role] = col_set[alias.lower()]
                break
    return resolved


def _parse_metadata_rows(
    df, notebook_items: dict[str, str],
) -> tuple[list[dict[str, Any]], dict[str, str | None]]:
    """Parse metadata DataFrame with auto-detected columns.
    Returns (rows, column_mapping)."""
    df = df.fillna("")
    col_map = _resolve_columns(list(df.columns))

    rows = []
    for _, r in df.iterrows():
        nb_raw = str(r.get(col_map["notebook_name"] or "__missing__", ""))
        nb_display = notebook_items.get(nb_raw, nb_raw)

        row: dict[str, Any] = {
            "table_name": str(r.get(col_map["table_name"] or "__missing__", "")),
            "notebook_id": nb_raw,
            "notebook_name": nb_display,
            "layer": str(r.get(col_map["layer"] or "__missing__", "")),
            "execution_order": _safe_int(r.get(col_map["execution_order"] or "__missing__", "")),
            "load_type": str(r.get(col_map["load_type"] or "__missing__", "")),
            "frequency": str(r.get(col_map["frequency"] or "__missing__", "")),
            "is_active": _safe_int(r.get(col_map["is_active"] or "__missing__", 1)),
        }
        # If layer is empty, try to detect from table name
        if not row["layer"] or row["layer"] == "":
            row["layer"] = _detect_layer(row["table_name"])
        rows.append(row)

    return rows, col_map


# ═══════════════════════════════════════════════════════════════════════════
# Tool 1: discover_lineage
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool
async def discover_lineage(
    ctx: Context,
    pipeline_names: Annotated[
        list[str] | None,
        "Pipeline names to analyze (e.g. ['pl_master_daily']). Omit to scan ALL pipelines."
    ] = None,
    include_metadata: Annotated[
        bool,
        "Read metadata table for notebook→table mapping. Set false if workspace has no metadata table."
    ] = True,
    metadata_table: Annotated[
        str,
        "Metadata table path in lakehouse. Auto-detects column schema."
    ] = "dbo/utl_pipeline_metadata",
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> dict[str, Any]:
    """Build COMPLETE workspace lineage in one call — the 'big picture' tool.

    USE THIS WHEN: You need to understand the full pipeline orchestration, see all tables
    and their notebooks, detect configuration gaps, or answer 'how does data flow in this workspace?'

    DO NOT USE when: You only need to debug one specific table (use trace_table instead),
    or you need to analyze local notebook code (use scan_local_folder).

    What it does:
    1. Lists all workspace items (pipelines + notebooks)
    2. Gets pipeline definitions → parses orchestration flow, activity dependencies
    3. Reads metadata table → maps notebooks to tables with layer, execution_order, load_type
    4. Auto-detects issues: orphan layers, missing notebooks, execution_order gaps

    Works with ANY Fabric workspace — auto-detects metadata schema and layer naming."""
    client = ctx.lifespan_context["http_client"]

    # --- Step 1: List workspace items ---
    try:
        items = await fabric_api.list_workspace_items(client, workspace_id)
    except Exception as e:
        raise ToolError(f"Failed to list workspace items: {e}")

    pipeline_items = {i["displayName"]: i["id"] for i in items if i["type"] == "DataPipeline"}
    notebook_items = {i["id"]: i["displayName"] for i in items if i["type"] == "Notebook"}

    # --- Step 2: Get pipeline definitions ---
    target_pipelines = pipeline_names or list(pipeline_items.keys())
    target_pipelines = [p for p in target_pipelines if p in pipeline_items]

    pipelines_parsed: dict[str, dict[str, Any]] = {}
    for pl_name in target_pipelines:
        pl_id = pipeline_items[pl_name]
        try:
            raw_def = await fabric_api.get_item_definition(
                client, pl_id, "DataPipeline", workspace_id,
            )
            decoded = fabric_api.decode_pipeline_payload(raw_def)
            if not decoded:
                continue
            props = decoded.get("properties", decoded)
            activities = _parse_activities(props.get("activities", []))
            nb_refs = _collect_all_notebook_ids(activities)
            lookup_filters = _extract_lookup_filters(activities)

            # Resolve notebook IDs → display names
            for ref in nb_refs:
                nid = ref["notebook_id"]
                if nid in notebook_items:
                    ref["notebook_name"] = notebook_items[nid]
                elif not ref["dynamic"]:
                    ref["notebook_name"] = nid

            # Detect child pipeline calls
            child_pipelines = []
            for act in activities:
                if act["type"] in ("ExecutePipeline", "InvokePipeline"):
                    cp_id = act.get("called_pipeline_id", "")
                    cp_name = act.get("called_pipeline", "")
                    if cp_id:
                        for pn, pid in pipeline_items.items():
                            if pid == cp_id:
                                cp_name = pn
                                break
                    child_pipelines.append({
                        "name": cp_name,
                        "depends_on": act.get("depends_on", []),
                    })

            pipelines_parsed[pl_name] = {
                "pipeline_id": pl_id,
                "activities": activities,
                "notebook_refs": nb_refs,
                "lookup_filters": lookup_filters,
                "child_pipelines": child_pipelines,
                "activity_count": len(activities),
            }
        except Exception as e:
            pipelines_parsed[pl_name] = {"pipeline_id": pl_id, "error": str(e)}

    # --- Step 3: Read metadata table ---
    metadata_rows: list[dict[str, Any]] = []
    col_map: dict[str, str | None] = {}
    if include_metadata:
        table_path = f"Tables/{metadata_table}"
        try:
            df = await delta.read_delta_table(client, table_path, workspace_id, lakehouse_id)
            if not df.empty:
                metadata_rows, col_map = _parse_metadata_rows(df, notebook_items)
        except Exception as e:
            col_map = {"_error": str(e)}  # type: ignore[assignment]

    # --- Step 4: Build orchestration + coverage analysis ---
    # Determine pipeline coverage from lookup filters
    pipeline_coverage: dict[str, list[str]] = {}
    for pl_name, pl_data in pipelines_parsed.items():
        if "error" in pl_data:
            continue
        for lf in pl_data.get("lookup_filters", []):
            layer = lf.get("layer", "")
            if layer:
                pipeline_coverage.setdefault(layer, []).append(pl_name)

    metadata_layers = sorted(set(r["layer"] for r in metadata_rows if r["layer"] and r["layer"] != "unknown"))
    covered_layers = set(pipeline_coverage.keys())
    uncovered_layers = sorted(set(metadata_layers) - covered_layers)

    # --- Step 5: Build orchestration output ---
    orchestration = []
    for pl_name, pl_data in pipelines_parsed.items():
        if "error" in pl_data:
            orchestration.append({"pipeline": pl_name, "error": pl_data["error"]})
            continue
        children = pl_data.get("child_pipelines", [])
        if children:
            flow_steps = []
            for ch in children:
                child_name = ch["name"]
                child_data = pipelines_parsed.get(child_name, {})
                child_filters = child_data.get("lookup_filters", [])
                # Match tables from metadata
                matched_tables = []
                for lf in child_filters:
                    for row in metadata_rows:
                        if _filter_matches(row, lf):
                            matched_tables.append(row["table_name"])
                flow_steps.append({
                    "pipeline": child_name,
                    "depends_on": [d["activity"] for d in ch["depends_on"]],
                    "lookup_filters": child_filters,
                    "table_count": len(matched_tables),
                    "tables": matched_tables,
                })
            orchestration.append({"pipeline": pl_name, "type": "master", "flow": flow_steps})
        else:
            orchestration.append({
                "pipeline": pl_name,
                "type": "worker",
                "lookup_filters": pl_data.get("lookup_filters", []),
            })

    # --- Step 6: Execution sequence (sorted by layer priority + exec order) ---
    layer_order_map: dict[str, dict[int, list[dict]]] = {}
    for row in metadata_rows:
        layer = row["layer"]
        order = row["execution_order"]
        if isinstance(order, int) and layer:
            layer_order_map.setdefault(layer, {}).setdefault(order, []).append(row)

    # Dynamic layer ordering based on execution_order values
    layer_min_order = {
        layer: min(orders.keys()) for layer, orders in layer_order_map.items()
    }
    sorted_layers = sorted(layer_min_order.keys(), key=lambda x: layer_min_order[x])

    execution_sequence = []
    for layer in sorted_layers:
        for order in sorted(layer_order_map[layer].keys()):
            tables = layer_order_map[layer][order]
            pl_name = pipeline_coverage.get(layer, ["(no pipeline)"])
            execution_sequence.append({
                "layer": layer,
                "execution_order": order,
                "pipeline": pl_name[0] if pl_name != ["(no pipeline)"] else "(no pipeline)",
                "table_count": len(tables),
                "tables": [
                    {
                        "table_name": t["table_name"],
                        "notebook": t["notebook_name"],
                        "load_type": t["load_type"],
                        "frequency": t["frequency"],
                    }
                    for t in sorted(tables, key=lambda x: x["table_name"])
                ],
            })

    return {
        "summary": {
            "total_pipelines": len(pipelines_parsed),
            "total_tables_in_metadata": len(metadata_rows),
            "total_notebooks_in_workspace": len(notebook_items),
            "layers_covered_by_pipelines": sorted(covered_layers),
            "layers_NOT_covered": uncovered_layers,
            "metadata_columns_detected": {k: v for k, v in col_map.items() if v},
        },
        "orchestration": orchestration,
        "execution_sequence": execution_sequence,
        "issues": _detect_issues(metadata_rows, pipelines_parsed, pipeline_coverage, notebook_items),
    }


def _filter_matches(row: dict, lookup_filter: dict) -> bool:
    """Check if a metadata row matches a lookup filter's conditions.
    Generic: matches any key that exists in both dicts."""
    for key, val in lookup_filter.items():
        if key in ("activity", "raw_query"):
            continue
        row_val = str(row.get(key, ""))
        if row_val and row_val != str(val):
            return False
    return True


def _detect_issues(
    metadata_rows: list[dict],
    pipelines_parsed: dict,
    pipeline_coverage: dict,
    notebook_items: dict,
) -> list[dict[str, str]]:
    """Auto-detect configuration issues and gaps."""
    issues = []

    # 1. Layers without pipeline coverage
    metadata_layers = set(r["layer"] for r in metadata_rows if r["layer"] and r["layer"] != "unknown")
    for layer in sorted(metadata_layers - set(pipeline_coverage.keys())):
        count = sum(1 for r in metadata_rows if r["layer"] == layer)
        issues.append({
            "severity": "warning",
            "type": "no_pipeline_coverage",
            "message": f"Layer '{layer}' has {count} tables in metadata but no pipeline queries for it",
        })

    # 2. Notebooks in metadata that don't exist in workspace
    for row in metadata_rows:
        nb_id = row.get("notebook_id", "")
        if nb_id and nb_id not in notebook_items and not nb_id.startswith(("@", "{")):
            issues.append({
                "severity": "error",
                "type": "missing_notebook",
                "message": f"Table '{row['table_name']}' → notebook '{nb_id}' not in workspace",
            })

    # 3. Missing execution_order
    for row in metadata_rows:
        if row.get("execution_order") == "" or row.get("execution_order") is None:
            issues.append({
                "severity": "warning",
                "type": "missing_execution_order",
                "message": f"Table '{row['table_name']}' has no execution_order",
            })

    # 4. Duplicate table names
    seen_tables: dict[str, int] = {}
    for row in metadata_rows:
        t = row["table_name"]
        seen_tables[t] = seen_tables.get(t, 0) + 1
    for t, count in seen_tables.items():
        if count > 1:
            issues.append({
                "severity": "warning",
                "type": "duplicate_table",
                "message": f"Table '{t}' appears {count} times in metadata",
            })

    # 5. Inactive tables summary
    inactive = [r["table_name"] for r in metadata_rows if r.get("is_active") == 0]
    if inactive:
        issues.append({
            "severity": "info",
            "type": "inactive_tables",
            "message": f"{len(inactive)} inactive: {', '.join(inactive)}",
        })

    return issues


# ═══════════════════════════════════════════════════════════════════════════
# Tool 2: trace_table
# ═══════════════════════════════════════════════════════════════════════════

@mcp.tool
async def trace_table(
    ctx: Context,
    table_name: Annotated[str, "Table name to trace (partial match supported, e.g. 'invoice_weekly')"],
    direction: Annotated[str, "'upstream' = what feeds this table, 'downstream' = what consumes it"] = "upstream",
    metadata_table: Annotated[str, "Metadata table path"] = "dbo/utl_pipeline_metadata",
    folder_path: Annotated[str | None, "Local folder with notebook code for confirmed source→target detection"] = None,
    workspace_id: Annotated[str | None, "Workspace GUID"] = None,
    lakehouse_id: Annotated[str | None, "Lakehouse GUID"] = None,
) -> dict[str, Any]:
    """Debug/trace a SPECIFIC table's data lineage.

    USE THIS WHEN: You need to answer 'where does this table's data come from?' (upstream)
    or 'what breaks if this table fails?' (downstream). Quick, targeted, one-table focus.

    DO NOT USE when: You need the full workspace picture (use discover_lineage),
    or you need to scan local code (use scan_local_folder).

    How tracing works:
    - Upstream: finds all tables with lower execution_order that could feed this table.
      If folder_path provided, parses notebook code to confirm direct dependencies.
    - Downstream: finds all tables with higher execution_order that could depend on this.
      If folder_path provided, confirms which notebooks actually read this table."""
    client = ctx.lifespan_context["http_client"]

    # --- Read metadata + notebook mapping ---
    table_path = f"Tables/{metadata_table}"
    try:
        df = await delta.read_delta_table(client, table_path, workspace_id, lakehouse_id)
    except Exception as e:
        raise ToolError(f"Failed to read metadata: {e}")
    if df.empty:
        raise ToolError("Metadata table is empty")

    try:
        items = await fabric_api.list_workspace_items(client, workspace_id)
    except Exception as e:
        raise ToolError(f"Failed to list workspace: {e}")

    notebook_items = {i["id"]: i["displayName"] for i in items if i["type"] == "Notebook"}
    metadata_rows, _ = _parse_metadata_rows(df, notebook_items)

    # --- Find matching table(s) ---
    search = table_name.lower()
    matched = [t for t in metadata_rows if search in t["table_name"].lower()]
    if not matched:
        all_names = sorted(set(t["table_name"] for t in metadata_rows))
        raise ToolError(f"No table matching '{table_name}'. Available:\n" + "\n".join(all_names))

    target = matched[0]

    # --- Parse local notebooks if folder provided ---
    nb_sources: dict[str, list[str]] = {}
    nb_targets: dict[str, list[str]] = {}
    if folder_path:
        root = Path(folder_path)
        if root.is_dir():
            for f in root.rglob("*"):
                if f.is_file() and f.suffix in (".ipynb", ".py"):
                    parsed = _parse_notebook_code(f)
                    nb_sources[f.stem] = parsed["sources"]
                    nb_targets[f.stem] = parsed["targets"]

    # --- Build trace ---
    if direction == "upstream":
        trace = _trace_upstream(target, metadata_rows, nb_sources)
    else:
        trace = _trace_downstream(target, metadata_rows, nb_sources)

    trace["matched_table"] = target
    if len(matched) > 1:
        trace["other_matches"] = [t["table_name"] for t in matched[1:]]
    return trace


def _trace_upstream(
    target: dict, all_tables: list[dict], nb_sources: dict,
) -> dict[str, Any]:
    """Trace upstream: what could produce data for this table?"""
    nb_name = target["notebook_name"]
    exec_order = target["execution_order"]

    sources_from_code = nb_sources.get(nb_name, [])
    has_code = bool(sources_from_code)

    upstream = []
    if isinstance(exec_order, int):
        for t in all_tables:
            t_order = t.get("execution_order", "")
            if isinstance(t_order, int) and t_order < exec_order:
                is_direct = any(
                    t["table_name"].lower() in s.lower() or s.lower() in t["table_name"].lower()
                    for s in sources_from_code
                ) if has_code else None
                entry = {
                    "table_name": t["table_name"],
                    "layer": t["layer"],
                    "execution_order": t_order,
                    "notebook": t["notebook_name"],
                }
                if has_code:
                    entry["confirmed_source"] = is_direct
                upstream.append(entry)

    # Sort: confirmed sources first, then by execution order
    upstream.sort(key=lambda x: (not x.get("confirmed_source", False), x.get("execution_order", 99)))

    return {
        "direction": "upstream",
        "producing_notebook": nb_name,
        "execution_order": exec_order,
        "layer": target["layer"],
        "sources_from_code": sources_from_code if has_code else "(no local code — pass folder_path to confirm)",
        "upstream_tables": upstream,
    }


def _trace_downstream(
    target: dict, all_tables: list[dict], nb_sources: dict,
) -> dict[str, Any]:
    """Trace downstream: what consumes this table?"""
    target_name = target["table_name"]
    exec_order = target["execution_order"]
    has_code = bool(nb_sources)

    # Which notebooks read this table?
    confirmed_consumers = []
    if has_code:
        for nb_name, sources in nb_sources.items():
            if any(target_name.lower() in s.lower() or s.lower() in target_name.lower() for s in sources):
                confirmed_consumers.append(nb_name)

    downstream = []
    if isinstance(exec_order, int):
        for t in all_tables:
            t_order = t.get("execution_order", "")
            if isinstance(t_order, int) and t_order > exec_order:
                entry = {
                    "table_name": t["table_name"],
                    "layer": t["layer"],
                    "execution_order": t_order,
                    "notebook": t["notebook_name"],
                }
                if has_code:
                    entry["confirmed_consumer"] = t["notebook_name"] in confirmed_consumers
                downstream.append(entry)

    downstream.sort(key=lambda x: (not x.get("confirmed_consumer", False), x.get("execution_order", 99)))

    return {
        "direction": "downstream",
        "target_table": target_name,
        "execution_order": exec_order,
        "layer": target["layer"],
        "confirmed_consumers": confirmed_consumers if has_code else "(pass folder_path to confirm)",
        "downstream_tables": downstream,
    }


# ═══════════════════════════════════════════════════════════════════════════
# Tool 3: scan_local_folder
# ═══════════════════════════════════════════════════════════════════════════

# Regex patterns — covers PySpark, Spark SQL, pandas, and common patterns
_READ_PATTERNS = [
    re.compile(r'''\.load\(\s*["']([^"']+)["']\s*\)'''),
    re.compile(r'''\.table\(\s*["']([^"']+)["']\s*\)'''),
    re.compile(r'''(?:spark\.sql|spark_sql|sql)\(\s*(?:f?["']{1,3})\s*SELECT\s+.*?\bFROM\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)''', re.IGNORECASE | re.DOTALL),
    re.compile(r'''\.(?:parquet|csv|json|orc|avro)\(\s*["']([^"']+)["']\s*\)'''),
    re.compile(r'''\bJOIN\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)\s''', re.IGNORECASE),
    # pandas: pd.read_csv/parquet/delta
    re.compile(r'''pd\.read_\w+\(\s*["']([^"']+)["']'''),
    # DeltaTable.forPath / forName
    re.compile(r'''DeltaTable\.for(?:Path|Name)\(\s*\w+\s*,\s*["']([^"']+)["']'''),
]

_WRITE_PATTERNS = [
    re.compile(r'''\.save\(\s*["']([^"']+)["']\s*\)'''),
    re.compile(r'''\.saveAsTable\(\s*["']([^"']+)["']\s*\)'''),
    re.compile(r'''(?:INSERT\s+(?:INTO|OVERWRITE)\s+)([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)''', re.IGNORECASE),
    re.compile(r'''MERGE\s+INTO\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)''', re.IGNORECASE),
    re.compile(r'''CREATE\s+(?:OR\s+REPLACE\s+)?TABLE\s+([a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*)\s+.*?AS\s+SELECT''', re.IGNORECASE | re.DOTALL),
    # df.to_parquet / to_csv
    re.compile(r'''\.to_(?:parquet|csv|delta)\(\s*["']([^"']+)["']'''),
]

_IGNORE_PREFIXES = ("http", "/mnt", "abfss", "{", "@", "spark_catalog", "hive_metastore", "#")


def _normalize_table_ref(ref: str) -> str:
    """Normalize table reference to canonical form."""
    ref = ref.strip().rstrip("/")
    if ref.startswith("Tables/"):
        ref = ref.replace("Tables/", "").replace("/", ".")
    if ref.startswith("dbo."):
        ref = ref[4:]
    return ref


def _parse_notebook_code(path: Path) -> dict[str, Any]:
    """Parse notebook file → extract source/target table references."""
    if path.suffix == ".ipynb":
        try:
            nb = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {"file": path.name, "notebook_name": path.stem, "layer": "unknown",
                    "sources": [], "targets": [], "error": "unreadable"}
        cells = nb.get("cells", [])
        code = "\n".join(
            "".join(c.get("source", [])) if isinstance(c.get("source"), list) else str(c.get("source", ""))
            for c in cells if c.get("cell_type") == "code"
        )
    else:
        try:
            code = path.read_text(encoding="utf-8")
        except OSError:
            return {"file": path.name, "notebook_name": path.stem, "layer": "unknown",
                    "sources": [], "targets": [], "error": "unreadable"}

    if not code:
        return {"file": path.name, "notebook_name": path.stem, "layer": _detect_layer(path.stem),
                "sources": [], "targets": []}

    sources: set[str] = set()
    targets: set[str] = set()

    for pat in _READ_PATTERNS:
        for m in pat.finditer(code):
            ref = _normalize_table_ref(m.group(1))
            if ref and len(ref) > 1 and not ref.startswith(_IGNORE_PREFIXES):
                sources.add(ref)

    for pat in _WRITE_PATTERNS:
        for m in pat.finditer(code):
            ref = _normalize_table_ref(m.group(1))
            if ref and len(ref) > 1 and not ref.startswith(_IGNORE_PREFIXES):
                targets.add(ref)

    sources -= targets  # don't report writes as reads

    return {
        "file": path.name,
        "notebook_name": path.stem,
        "layer": _detect_layer(path.stem),
        "sources": sorted(sources),
        "targets": sorted(targets),
    }


@mcp.tool
async def scan_local_folder(
    folder_path: Annotated[str, "Absolute path to local folder with Fabric notebooks (.ipynb/.py)"],
) -> dict[str, Any]:
    """Parse LOCAL notebook code to extract source→target table references.

    USE THIS WHEN: You have notebook source code on disk and want to know what tables
    each notebook reads/writes. Useful for code review, pre-deployment validation,
    or enriching workspace lineage with actual data flow details.

    DO NOT USE when: You need workspace-level orchestration info (use discover_lineage),
    or you need to trace one table (use trace_table).

    NOTE: Engine-pattern notebooks that receive table names via parameters will only show
    the variable names (e.g. 'raw_source'), not actual table names. For those, use
    discover_lineage which reads the metadata table for actual mappings."""
    root = Path(folder_path)
    if not root.is_dir():
        raise ToolError(f"Folder not found: {folder_path}")

    notebooks: list[dict[str, Any]] = []
    by_layer: dict[str, int] = {}
    all_sources: set[str] = set()
    all_targets: set[str] = set()

    for f in sorted(root.rglob("*")):
        if not f.is_file() or f.suffix not in (".ipynb", ".py"):
            continue
        parsed = _parse_notebook_code(f)
        layer = parsed.get("layer", "unknown")
        by_layer[layer] = by_layer.get(layer, 0) + 1
        all_sources.update(parsed.get("sources", []))
        all_targets.update(parsed.get("targets", []))
        notebooks.append(parsed)

    # Bonus: detect pipeline JSON files
    pipeline_files = []
    for f in sorted(root.rglob("*.json")):
        if not f.is_file():
            continue
        try:
            raw = json.loads(f.read_text(encoding="utf-8"))
            if "activities" in raw.get("properties", raw):
                pipeline_files.append(f.name)
        except (json.JSONDecodeError, OSError):
            pass

    return {
        "folder": str(root),
        "summary": {
            "total_notebooks": len(notebooks),
            "notebooks_by_layer": dict(sorted(by_layer.items())),
            "unique_source_tables": len(all_sources),
            "unique_target_tables": len(all_targets),
            "root_sources": sorted(all_sources - all_targets),
            "terminal_targets": sorted(all_targets - all_sources),
            "pipeline_files_found": len(pipeline_files),
        },
        "notebooks": notebooks,
        "pipeline_files": pipeline_files,
    }
