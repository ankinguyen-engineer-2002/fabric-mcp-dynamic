# Analytics Fabric MCP Dynamic

Full automated pipeline scan, metadata extraction, visualization, and deployment using MCP server `fabric-dynamic`.
Reproduces the 6-phase workflow from the original SKILL.md — but via MCP tool calls instead of inline Python.

## Instructions

Execute the following phases **sequentially**. After each phase, report the result before proceeding to the next. If any phase fails, stop and report the error — do not skip phases.

---

### PHASE 1: AUTH & CONNECTIVITY
Call `health_check` from MCP server `fabric-dynamic`.
- Confirm: auth OK, workspace reachable, lakehouse/warehouse IDs valid.
- If fail → tell user to run `az login` and stop.

---

### PHASE 2: SCAN WORKSPACE
Call `scan_workspace`.
- List all items: DataPipelines, Notebooks, Lakehouses, Warehouses, etc.
- Print summary table: item type + count.

---

### PHASE 3: EXTRACT METADATA FROM DELTA TABLE
Call `read_delta_table` with table name `utl_pipeline_metadata`.
- Server handles internally: checkpoint discovery → parquet download → delta log replay → parse rows.
- Validate: expect **26 rows** (9 REF + 7 BRZ + 8 SLV + 2 GLD). If count differs, warn but continue.
- Print layer breakdown table.

Then in parallel:
- Call `get_pipeline_def` for pipeline `pl_master_daily` — extract pipeline hierarchy.
- Call `get_table_schema` for table `utl_pipeline_metadata` — confirm schema.
- Call `get_row_count` for key tables across layers — collect row counts for viz.

---

### PHASE 4: DATA QUALITY VALIDATION
Run these in parallel:
- Call `run_dq_check` — execute quality rules against metadata.
- Call `validate_row_counts` — cross-layer row count validation.
- Call `check_freshness` — verify data is not stale.

Print DQ summary: pass/fail counts, any issues found.
If critical DQ failures → warn user but continue to viz.

---

### PHASE 5: GENERATE VISUALIZATION & DEPLOY
1. Call `generate_viz` — pass metadata from Phase 3 + pipeline hierarchy. Build D3.js pipeline decomposition tree.
2. Call `preview_local` — start local server and provide localhost URL.
3. Ask user: **"Deploy to GitHub Pages? (y/n)"**
   - If yes → call `deploy_github_pages`.
   - If no → skip deployment.

---

### PHASE 6: FINAL REPORT
Print summary report:

```
=== Fabric Pipeline Scan Report ===
Workspace:    [name]
Lakehouse:    [name]
Environment:  [dev/prod]

Layer Breakdown:
  REF: [x] tables — [row count]
  BRZ: [x] tables — [row count]
  SLV: [x] tables — [row count]
  GLD: [x] tables — [row count]
  Total: 26 tables — [total rows]

DQ Status:    [x/y passed]
Freshness:    [OK/STALE]

Preview:      http://localhost:8080
GitHub Pages: [URL if deployed]
```

---

## Notes
- All tool calls use MCP server `fabric-dynamic`. No inline Python code needed.
- If MCP server is not connected, tell user to check their MCP config in `~/.claude.json`.
- This skill works from any project directory as long as `fabric-dynamic` MCP server is configured.
