"""Visualization tools — generate D3.js tree, preview, deploy to GitHub Pages."""

from __future__ import annotations

import json
import os
import re
import subprocess
from typing import Annotated, Any

from fastmcp import Context
from fastmcp.exceptions import ToolError

from fabric_mcp_dynamic.server import mcp


@mcp.tool
async def generate_viz(
    ctx: Context,
    metadata: Annotated[list[dict[str, Any]], "Pipeline metadata rows from get_metadata tool"],
    template_path: Annotated[str | None, "Path to HTML template. If omitted, generates standalone."] = None,
    output_path: Annotated[str | None, "Output HTML path. Defaults to ./pipeline_workflow.html"] = None,
) -> dict[str, Any]:
    """Generate a D3.js pipeline decomposition tree from metadata.
    Expects metadata in format: [{layer, table, load, freq, rows, status, nb, runtime}, ...]."""
    # Build JS DATA array
    js_entries = []
    for row in metadata:
        entry = (
            f"{{layer:'{row.get('layer', '')}',order:{row.get('execution_order', 0)},"
            f"table:'{row.get('table_name', '')}',load:'{row.get('load_type', '')}',freq:'{row.get('frequency', '')}'"
            f",rows:{row.get('rows_loaded', 0)},status:'{row.get('status', '')}'"
            f",nb:'{row.get('notebook_name', '')}',runtime:'{row.get('runtime', 'n/a')}'}}"
        )
        js_entries.append(entry)

    data_block = f"const DATA=[{','.join(js_entries)}];"

    # Calculate stats
    total_rows = sum(r.get("rows_loaded", 0) for r in metadata)
    success_count = sum(1 for r in metadata if r.get("status") == "success")

    if total_rows >= 1_000_000:
        rows_display = f"~{total_rows // 1_000_000}M"
    elif total_rows >= 1_000:
        rows_display = f"~{total_rows // 1_000}K"
    else:
        rows_display = str(total_rows)

    output = output_path or "./pipeline_workflow.html"

    if template_path and os.path.exists(template_path):
        # Update existing template
        with open(template_path, "r") as f:
            html = f.read()

        # Replace DATA block
        html = re.sub(
            r"const DATA=\[.*?\];",
            data_block,
            html,
            flags=re.DOTALL,
        )

        # Update sidebar stats if present
        html = re.sub(r"~\d+[MK]?\s*rows", f"{rows_display} rows", html)
        html = re.sub(r"\d+/\d+\s*success", f"{success_count}/{len(metadata)} success", html)

        with open(output, "w") as f:
            f.write(html)

        return {
            "status": "updated",
            "output": output,
            "tables": len(metadata),
            "total_rows": rows_display,
            "success_rate": f"{success_count}/{len(metadata)}",
        }
    else:
        # Generate minimal standalone HTML
        html = _generate_standalone_html(data_block, metadata, rows_display, success_count)
        with open(output, "w") as f:
            f.write(html)

        return {
            "status": "generated",
            "output": output,
            "tables": len(metadata),
            "total_rows": rows_display,
            "success_rate": f"{success_count}/{len(metadata)}",
        }


def _generate_standalone_html(
    data_block: str, metadata: list[dict], rows_display: str, success_count: int,
) -> str:
    """Generate a minimal standalone HTML visualization."""
    layers = {}
    for r in metadata:
        layer = r.get("layer", "?")
        layers.setdefault(layer, []).append(r)

    table_rows = ""
    for layer in ["REF", "BRZ", "SLV", "GLD"]:
        tables = layers.get(layer, [])
        for t in tables:
            status_dot = "green" if t.get("status") == "success" else "red"
            table_rows += (
                f"<tr><td>{layer}</td><td>{t.get('table_name','')}</td>"
                f"<td>{t.get('load_type','')}</td><td>{t.get('rows_loaded',0):,}</td>"
                f"<td><span style='color:{status_dot}'>&#9679;</span> {t.get('status','')}</td>"
                f"<td>{t.get('runtime','n/a')}</td></tr>\n"
            )

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Pipeline Metadata</title>
<style>body{{font-family:system-ui;background:#0d1525;color:#e2e8f0;padding:20px}}
table{{border-collapse:collapse;width:100%}}th,td{{padding:8px 12px;text-align:left;border-bottom:1px solid #1e293b}}
th{{color:#94a3b8;font-size:12px;text-transform:uppercase}}</style></head>
<body>
<h2>Pipeline Metadata — {len(metadata)} tables, {rows_display} rows, {success_count}/{len(metadata)} success</h2>
<script>{data_block}</script>
<table><tr><th>Layer</th><th>Table</th><th>Load</th><th>Rows</th><th>Status</th><th>Runtime</th></tr>
{table_rows}</table>
</body></html>"""


@mcp.tool
async def preview_local(
    ctx: Context,
    html_path: Annotated[str, "Path to the HTML file to preview"] = "./pipeline_workflow.html",
    port: Annotated[int, "Local server port"] = 8080,
) -> dict[str, str]:
    """Start a local HTTP server and open the visualization in browser."""
    import os

    directory = os.path.dirname(os.path.abspath(html_path))
    filename = os.path.basename(html_path)

    subprocess.Popen(
        ["python3", "-m", "http.server", str(port)],
        cwd=directory,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://localhost:{port}/{filename}"
    subprocess.run(["open", url], check=False)

    return {"url": url, "status": "server started"}


@mcp.tool
async def deploy_github_pages(
    ctx: Context,
    repo_path: Annotated[str, "Path to the GitHub Pages repo"],
    source_html: Annotated[str, "Path to the HTML file to deploy"] = "./pipeline_workflow.html",
    target_file: Annotated[str, "Target filename in repo"] = "index.html",
    commit_message: Annotated[str | None, "Custom commit message"] = None,
) -> dict[str, str]:
    """Copy HTML to a GitHub Pages repo, commit, and push."""
    import shutil
    from datetime import date

    target_path = os.path.join(repo_path, target_file)
    shutil.copy2(source_html, target_path)

    msg = commit_message or f"Update pipeline metadata with live data ({date.today().isoformat()})"

    try:
        subprocess.run(["git", "add", target_file], cwd=repo_path, check=True, capture_output=True)
        subprocess.run(["git", "commit", "-m", msg], cwd=repo_path, check=True, capture_output=True)
        subprocess.run(["git", "push", "origin", "main"], cwd=repo_path, check=True, capture_output=True)
        return {"status": "deployed", "file": target_file, "message": msg}
    except subprocess.CalledProcessError as e:
        raise ToolError(f"Git deploy failed: {e.stderr.decode() if e.stderr else str(e)}")
