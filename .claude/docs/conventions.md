# {Project} — Naming & Convention Reference

## Table Naming
| Prefix | Layer | Example |
|---|---|---|
| brz_ | Bronze | brz_sales_invoice_detail |
| slv_ | Silver | slv_sales_history |
| gld_ | Gold | gld_demand_forecast |
| ref_ | Reference/Lookup | ref_product_master |
| utl_ | Utility/Metadata | utl_pipeline_metadata |
| stg_ | Staging (temp) | stg_raw_import |
| dim_ | Warehouse Dimension | dim_product |
| fact_ | Warehouse Fact | fact_sales |

## Column Naming Prefixes
| Prefix | Type | Example |
|---|---|---|
| id_ | Identifier/Key | id_product, id_order |
| dt_ | Date | dt_invoice, dt_ship |
| ts_ | Timestamp | ts_loaded_at, ts_updated |
| amt_ | Amount/Money | amt_revenue, amt_cost |
| qty_ | Quantity | qty_ordered, qty_shipped |
| code_ | Code/Short string | code_warehouse, code_status |
| flag_ | Boolean (0/1) | flag_active, flag_processed |
| nm_ | Name/Description | nm_product, nm_customer |

## Notebook Naming
- nb_{layer}_{source}__{description}
- Example: nb_brz_saleshistory_afi__invoicedetail

## Anti-Patterns to Avoid
{List project-specific things that are forbidden or cause issues}
