# {Project} — Data Dictionary

## Bronze Tables
| Table | Source | Load Strategy | Rows (approx) | Key Columns |
|---|---|---|---|---|
| brz_example | SharePoint CSV | incremental | ~1M | id_record, dt_date |

## Silver Tables
| Table | Depends On | Purpose |
|---|---|---|
| slv_example | brz_example | Cleaned + joined foundation |

## Gold Tables
| Table | Grain | Consumers |
|---|---|---|
| gld_example | SKU x Week | Power BI supply chain report |

## Key Business Rules
{Document business logic that isn't obvious from column names}
