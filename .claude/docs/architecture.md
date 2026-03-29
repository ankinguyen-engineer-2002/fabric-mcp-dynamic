# {Project} — Architecture Reference

## Data Flow
{Paste or describe your actual data flow diagram here}
{Source systems → ingestion layer → transform layers → serving layer → BI}

## Layer Definitions
### Bronze ({prefix}_)
- Responsible for: {copy, rename, cast, trim}
- Not responsible for: {joins, aggregations, business logic}
- Load strategies used: {full / incremental / upsert / merge_monthly}

### Silver ({prefix}_)  
- Responsible for: {shared foundation, heavy joins, immutable calculations}
- Consumers: {all downstream Gold tables, Warehouse}

### Gold ({prefix}_)
- Responsible for: {project-specific aggregations, custom grain}
- Consumers: {Warehouse, specific Power BI reports}

### Warehouse
- Source: {any clean table, case by case}
- Responsible for: {last-mile reshape, RLS, column selection}

## Key Tables
{List the most important tables that Claude needs to know about}

## Pipeline Topology
{pl_master → pl_brz + pl_ref (parallel) → DQ → pl_slv → DQ → pl_gld → Warehouse}
