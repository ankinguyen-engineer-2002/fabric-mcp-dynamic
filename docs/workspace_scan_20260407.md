# Workspace DEV — Full Pipeline & Lineage Scan

> **Scan date:** 2026-04-07
> **Workspace:** `c8d9fc83-18b6-4e1d-8264-0b49eed36fe0`
> **Lakehouse:** SupplyChain_Lakehouse (`62a3081e-4093-4f46-856c-f50aa58732fa`)
> **Warehouse:** SupplyChain_Warehouse (`e146ffe2-d907-46a7-9b7e-3e739a31b24e`)
> **Environment:** dev

---

## 1. Workspace Overview

| Item Type | Count | Highlights |
|-----------|------:|------------|
| Notebook | 91 | 39 active, 52 `remove_*` (deprecated) |
| Dataflow | 18 | Legacy Gen1 flows |
| DataPipeline | 11 | 1 master + 4 layer + 6 others |
| Warehouse | 4 | SupplyChain_Warehouse (primary) |
| Lakehouse | 3 | SupplyChain_Lakehouse (primary) |
| SemanticModel | 3 | Supply Chain Control Tower, SupplyChain_Gold, temp_SCPModel |
| SQLEndpoint | 3 | Auto-generated from Lakehouses |
| Report | 1 | Forecast Accuracy Gold |
| **Total** | **134** | |

---

## 2. Master Orchestration — `pl_master_daily`

Standard daily pipeline. Chay tuan tu 5 buoc, moi buoc phai thanh cong truoc khi buoc tiep theo bat dau.

```
pl_master_daily
│
├─ [1] pl_brz_daily ─────────────── BRZ layer (execution_order = 1)
│       │  Lookup: WHERE layer='BRZ' AND execution_order=1
│       │          AND next_run_time <= CURRENT_TIMESTAMP
│       └─ ForEach (batch=3) ──→ notebook per table
│
├─ [2] pl_slv_daily ─────────────── SLV layer (execution_order = 2 → 3)
│       │  Lookup_order2 → ForEach (batch=3)
│       │  Lookup_order3 → ForEach (batch=3)
│       └─ (2 waves: order=2 first, then order=3)
│
├─ [3] pl_gld_daily ─────────────── GLD layer (execution_order = 5)
│       │  Lookup: WHERE layer='GLD' AND execution_order=5
│       └─ ForEach (batch=3) ──→ notebook per table
│
├─ [4] pl_stored_procedure ──────── Warehouse SP (10+ parallel)
│       └─ usp_Load_Dim_Calendar
│          usp_Load_Dim_Customer_Grouping
│          usp_Load_Dim_Forecast_Horizon
│          usp_Load_Dim_Product
│          ... (all run parallel → SupplyChain_Warehouse)
│
└─ [5] Control Tower ────────────── Semantic Model Refresh
        └─ PBI Refresh → Supply Chain Control Tower
```

**Pattern:** Metadata-driven — moi pipeline query `dbo.utl_pipeline_metadata` de lay danh sach notebook can chay, ko hardcode.

---

## 3. Pipeline Metadata — 27 Tables

> Source: `dbo.utl_pipeline_metadata`
> All 27 tables: **status = success**

### 3.1 BRZ Layer (7 tables)

| # | Table | Load Type | Rows | Source (External) |
|---|-------|-----------|-----:|-------------------|
| 1 | `brz_saleshistory_afi__invoicedetail` | overwrite | 35,772,986 | SalesHistory_AFI / InvoiceDetail |
| 2 | `brz_saleshistory_afi__invoiceheader` | overwrite | 4,051,783 | SalesHistory_AFI / InvoiceHeader |
| 3 | `brz_supplychain_enh_1__demandforecastsnapshotdaily` | **incremental** | 0 | SupplyChain_Enh_1 / DemandForecastSnapshotDaily |
| 4 | `brz_wholesale_codis_afi__codatan` | overwrite | 918,213 | Wholesale_Codis_AFI / codatan |
| 5 | `brz_wholesale_codis_afi__comast` | overwrite | 229,910 | Wholesale_Codis_AFI / COMAST |
| 6 | `brz_wholesale_codis_afi__extord` | overwrite | 229,774 | Wholesale_Codis_AFI / EXTORD |
| 7 | `brz_wholesale_codis_afi__extorit` | overwrite | 912,132 | Wholesale_Codis_AFI / EXTORIT |

### 3.2 REF Layer (10 tables)

| # | Table | Frequency | Rows | Source (External) |
|---|-------|-----------|-----:|-------------------|
| 1 | `ref_calendar` | Monthly | 21,551 | MasterData_DW / DimDate |
| 2 | `ref_customer_account` | Monthly | 35,550 | Customers / AccountMaster |
| 3 | `ref_customer_account_group` | Daily | 35,439 | Wholesale_ProductSourcing_AFI / CustomerGrouping |
| 4 | `ref_customer_grouping` | Monthly | 9 | Wholesale_ProductSourcing_AFI / CustomerGrouping |
| 5 | `ref_customer_shipping_location` | Monthly | 127,421 | Customers / ShippingLocations |
| 6 | `ref_forecast_horizon` | Monthly | 8 | manual (hardcode) |
| 7 | `ref_item_master` | Monthly | 379,236 | MasterData_DW / DimItemMaster |
| 8 | `ref_order_type` | Monthly | 29 | Wholesale_Codis_AFI / AAORDTYP |
| 9 | `ref_product` | Monthly | 373,326 | SupplyChain_DW / DimCurrentProductDetails |
| 10 | `ref_warehouse` | Daily | 55 | SupplyChain_DW / DimAFIWarehouses |

### 3.3 SLV Layer (8 tables)

| # | Table | Order | Rows | Source Tables (Lakehouse) |
|---|-------|:-----:|-----:|--------------------------|
| 1 | `slv_invoice_detail_line_level` | 2 | 86,760,577 | brz_invoicedetail, brz_invoiceheader, ref_customer_account_group |
| 2 | `slv_open_order_line_level` | 2 | 307,016 | brz_codatan, brz_comast, brz_extord, brz_extorit, ref_item_master, ref_order_type |
| 3 | `slv_forecast_demand_monthly` | 2 | 38,404,417 | brz_demandforecastsnapshot, ref_calendar, ref_forecast_cycle |
| 4 | `slv_actual_demand_monthly` | 3 | 3,546,021 | ref_calendar, ref_customer_account_group, slv_invoice_detail, slv_open_order |
| 5 | `slv_actual_demand_weekly` | 3 | 9,713,179 | ref_calendar, ref_customer_account_group, slv_invoice_detail, slv_open_order |
| 6 | `slv_invoice_weekly` | 3 | 36,781,634 | ref_calendar, slv_invoice_detail_line_level |
| 7 | `slv_open_order_monthly` | 3 | 127,924 | ref_calendar, ref_customer_account_group, slv_open_order_line_level |
| 8 | `slv_naive_forecast_monthly` | 4 | 2,738,159 | ref_calendar, slv_actual_demand_monthly |

### 3.4 GLD Layer (2 tables)

| # | Table | Order | Rows | Source Tables (Lakehouse) |
|---|-------|:-----:|-----:|--------------------------|
| 1 | `gld_flat_forecast_actual` | 5 | 44,678,168 | slv_actual_demand_monthly, slv_forecast_demand_monthly, slv_naive_forecast_monthly |
| 2 | `gld_forecast_kpi_metric` | 5 | 75,077,835 | ref_forecast_horizon, slv_actual_demand_monthly, slv_forecast_demand_monthly, slv_naive_forecast_monthly |

---

## 4. Data Lineage — End-to-End Flow

```
                          ┌──────────────────────────────────────┐
                          │         EXTERNAL SOURCES             │
                          │                                      │
                          │  SalesHistory_AFI    (Invoice D+H)   │
                          │  SupplyChain_Enh_1   (Forecast)      │
                          │  Wholesale_Codis_AFI (Orders)        │
                          │  MasterData_DW       (Dims)          │
                          │  Customers           (Accounts)      │
                          └──────────────┬───────────────────────┘
                                         │
                    ┌────────────────────┬┴───────────���────────┐
                    ▼                    ▼                     ▼
        ┌───────────────────┐ ┌──────────────────┐ ┌──────────────────┐
        │  BRZ (order=1)    │ │  REF (order=1)   │ │  BRZ (order=1)   │
        │                   │ │                  │ │                   │
        │ brz_invoice_det   │ │ ref_calendar     │ │ brz_codatan      │
        │ brz_invoice_hdr   │ │ ref_item_master  │ │ brz_comast       │
        │ brz_forecast_snap │ │ ref_order_type   │ │ brz_extord       │
        │                   │ │ ref_cust_acct_grp│ │ brz_extorit      │
        │                   │ │ ref_cust_acct    │ │                   │
        │                   │ │ ref_product      │ │                   │
        │                   │ │ ref_warehouse    │ │                   │
        │                   │ │ ref_fcst_horizon │ │                   │
        └───────┬───────────┘ └────────┬─────────┘ └────────┬─────────┘
                │                      │                     │
                ▼                      ▼                     ▼
        ┌──────────────────────────────────────────────────────────────┐
        │                    SLV — Wave 1 (order=2)                    │
        │                                                              │
        │  brz_inv_det + brz_inv_hdr + ref_cust_grp                   │
        │      └──→ slv_invoice_detail_line_level  (86.7M rows)       │
        │                                                              │
        │  brz_codatan + brz_comast + brz_ext* + ref_item + ref_order │
        │      └──→ slv_open_order_line_level      (307K rows)        │
        │                                                              │
        │  brz_forecast_snap + ref_calendar + ref_forecast_cycle       │
        │      └──→ slv_forecast_demand_monthly    (38.4M rows)       │
        └──────────────────────────┬───────────────────────────────────┘
                                   │
                                   ▼
        ┌──────────────────────────────────────────────────────────────┐
        │                    SLV — Wave 2 (order=3)                    │
        │                                                              │
        │  slv_invoice_det + ref_cal                                   │
        │      └──→ slv_invoice_weekly             (36.7M rows)       │
        │                                                              │
        │  slv_invoice_det + slv_open_order + ref_cal + ref_cust_grp  │
        │      └──→ slv_actual_demand_monthly      (3.5M rows)        │
        │      └──→ slv_actual_demand_weekly        (9.7M rows)       │
        │                                                              │
        │  slv_open_order + ref_cal + ref_cust_grp                    │
        │      └──→ slv_open_order_monthly         (127K rows)        │
        └──────────────────────────┬───────────────────────────────────┘
                                   │
                                   ▼
        ┌──────────────────────────────────────────────────────────────┐
        │                    SLV — Wave 3 (order=4)                    │
        │                                                              │
        │  slv_actual_demand_monthly + ref_calendar                    │
        │      └──→ slv_naive_forecast_monthly     (2.7M rows)        │
        └──────────────────────────┬───────────────────────────────────┘
                                   │
                                   ▼
        ┌──────────────────────────────────────────────────────────────┐
        │                    GLD (order=5)                              │
        │                                                              │
        │  slv_actual + slv_forecast + slv_naive                       │
        │      └──→ gld_flat_forecast_actual       (44.6M rows)       │
        │                                                              │
        │  slv_actual + slv_forecast + slv_naive + ref_fcst_horizon   │
        │      └──→ gld_forecast_kpi_metric        (75.0M rows)       │
        └──────────────────────────┬───────────────────────────────────┘
                                   │
                                   ▼
        ┌──────────────────────────────────────────────────────────────┐
        │              STORED PROCEDURES → Warehouse                   │
        │                                                              │
        │  usp_Load_Dim_Calendar         usp_Load_Dim_Product         │
        │  usp_Load_Dim_Customer_Grouping  usp_Load_Dim_Forecast_Horizon│
        │  ... (10+ SPs run in parallel)                               │
        └──────────────────────────┬───────────────────────────────────┘
                                   │
                                   ▼
        ┌──────────────────────────────────────────────────────────────┐
        │              SEMANTIC MODEL REFRESH                          │
        │                                                              │
        │  Supply Chain Control Tower (PBI Dataset Refresh)            │
        └──────────────────────────────────────────────────────────────┘
```

---

## 5. Active Notebooks (39)

### Engine Notebooks (reusable)
| Notebook | Purpose |
|----------|---------|
| `brz_engine` | Generic BRZ ingestion engine |
| `slv_engine` | Generic SLV transformation engine |
| `gld_engine` | Generic GLD aggregation engine |
| `sp_engine` | Stored procedure sync engine |
| `env_config` | Environment configuration |

### BRZ Notebooks (6)
| Notebook | Target Table |
|----------|-------------|
| `nb_brz_SalesHistory_AFI__InvoiceDetail` | brz_saleshistory_afi__invoicedetail |
| `nb_brz_SalesHistory_AFI__InvoiceHeader` | brz_saleshistory_afi__invoiceheader |
| `nb_brz_SupplyChain_Enh_1__DemandForecastSnapshotDaily` | brz_supplychain_enh_1__demandforecastsnapshotdaily |
| `nb_brz_Wholesale_Codis_AFI__COMAST` | brz_wholesale_codis_afi__comast |
| `nb_brz_Wholesale_Codis_AFI__EXTORD` | brz_wholesale_codis_afi__extord |
| `nb_brz_Wholesale_Codis_AFI__EXTORIT` | brz_wholesale_codis_afi__extorit |

### REF Notebooks (9)
| Notebook | Target Table |
|----------|-------------|
| `nb_ref_calendar` | ref_calendar |
| `nb_ref_customer_account` | ref_customer_account |
| `nb_ref_customer_account_group` | ref_customer_account_group |
| `nb_ref_customer_grouping` | ref_customer_grouping |
| `nb_ref_customer_shipping_location` | ref_customer_shipping_location |
| `nb_ref_forecast_horizon` | ref_forecast_horizon |
| `nb_ref_item_master` | ref_item_master |
| `nb_ref_order_type` | ref_order_type |
| `nb_ref_product` | ref_product |

### SLV Notebooks (8)
| Notebook | Target Table |
|----------|-------------|
| `nb_slv_actual_demand_monthly` | slv_actual_demand_monthly |
| `nb_slv_actual_demand_weekly` | slv_actual_demand_weekly |
| `nb_slv_forecast_demand_monthly` | slv_forecast_demand_monthly |
| `nb_slv_invoice_detail_line_level` | slv_invoice_detail_line_level |
| `nb_slv_invoice_weekly` | slv_invoice_weekly |
| `nb_slv_naive_forecast_monthly` | slv_naive_forecast_monthly |
| `nb_slv_open_order_line_level` | slv_open_order_line_level |
| `nb_slv_open_order_monthly` | slv_open_order_monthly |

### GLD Notebooks (2)
| Notebook | Target Table |
|----------|-------------|
| `nb_gld_flat_forecast_actual` | gld_flat_forecast_actual |
| `nb_gld_forecast_kpi_metric` | gld_forecast_kpi_metric |

### Utility Notebooks
| Notebook | Purpose |
|----------|---------|
| `nb_utl_pipeline_metadata` | Pipeline metadata management |
| `nb_ult_lineage` | Lineage tracking |
| `nb_ref_warehouse` | Warehouse dimension |
| `nb_sp_sync_all` | Stored procedure sync |
| `data_dictionary_app` | Data dictionary generator |
| `Refresh SQL Endpoint Metadata` | SQL endpoint refresh |
| `Schedule_Update` | Schedule management |

---

## 6. Key Observations

| # | Finding | Detail |
|---|---------|--------|
| 1 | All tables healthy | 27/27 tables `status = success` |
| 2 | Metadata-driven | Pipeline ko hardcode — query `utl_pipeline_metadata` at runtime |
| 3 | Only 1 incremental | `brz_demandforecastsnapshot` (watermark: `dfcSnapshot = 2026-02-24`) |
| 4 | Largest tables | `slv_invoice_detail` (86.7M), `gld_forecast_kpi` (75M), `gld_flat_forecast` (44.6M) |
| 5 | 52 deprecated notebooks | Prefixed `remove_*` — candidates for cleanup |
| 6 | Lineage table stale | `utl_lineage` only has Semantic Model mappings (2026-03-18), real lineage is in `source_tables` column |
| 7 | Execution order gap | Order jumps 1 → 2 → 3 → 4 → 5 (no order=4 in GLD, order=4 only for `slv_naive_forecast`) |
| 8 | Batch concurrency | ForEach `batchCount=3` across all layer pipelines |
| 9 | **FIXED** metadata bug | `ref_customer_account_group` had wrong `notebook_name` (pointed to `nb_brz_InvoiceDetail`) and wrong `layer=BRZ`. Fixed to correct GUID `3efdaa82-...` (`nb_ref_customer_account_group`) and `layer=REF`. Delta log version 359. |

---

## 7. Pipeline IDs (Quick Reference)

| Pipeline | ID |
|----------|----|
| `pl_master_daily` | `4214332e-392f-4d2e-ac11-99094ac33aa7` |
| `pl_brz_daily` | `e388c9ef-f067-4e95-91b2-961436b8a683` |
| `pl_slv_daily` | `188d52f4-ff31-4459-904d-ea3a6e89b87f` |
| `pl_gld_daily` | `8a8fabf1-634e-4705-9909-929b5b451438` |
| `pl_stored_procedure` | `6577f34d-5765-4232-a045-9594017100c5` |
| `pl_master_regional` | `f743db73-610c-40fd-a8a7-09be823eca51` |
| `pl_regional_forecast` | `7370cb2a-15c8-4fb9-8714-a4ce8c1d0fc7` |
| `pl_forecast` | `95d8f2a2-d1f9-4c82-8ee9-ccd4c41891ff` |
| `pl_test_concurrency` | `7e0c18d2-1656-4b64-9642-d1787e9ce78c` |

---

*Generated by fabric-mcp-dynamic scan on 2026-04-07*
