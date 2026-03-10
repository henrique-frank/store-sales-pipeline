# System Design — Store Sales Pipeline

## Architecture Overview

The pipeline follows a **Bronze / Silver / Gold** medallion architecture on Snowflake, with Python handling ingestion and dbt Core (via dbt Cloud) handling all transformations.

```
  inbox/                Snowflake                          dbt Cloud
 ┌──────────┐    ┌─────────────────────────────────────────────────────────┐
 │ CSVs     │    │  BRONZE          SILVER              GOLD              │
 │ stores_* │───>│  stores_raw ───> dim_store ─────────> output3 (top5)   │
 │ sales_*  │───>│  sales_raw ────> fact_sales ────────> output1 (batch)  │
 │          │    │  ingestion_log   sales_rejected       output2 (tx day) │
 └──────────┘    └─────────────────────────────────────────────────────────┘
       │                Python PUT + COPY INTO       dbt incremental merge
       v
  archive/
```

## Processing Flow

### Stage 1: Discover

The Python ingestion script scans the configured `inbox/` directory for files matching `stores_<YYYYMMDD>.csv` or `sales_<YYYYMMDD>.csv`. The batch date is extracted from the filename. Files not matching either pattern are ignored.

### Stage 2: Idempotency Check

Before processing, a SHA-256 content hash is computed for each file. The hash is checked against `BRONZE.INGESTION_LOG`. If already present, the file is skipped. This prevents duplicate processing when files are re-delivered or the pipeline is re-run.

### Stage 3: Header Detection

CSV files may or may not contain a header row. The ingestion script reads the first line and checks if it contains known column names (e.g., `store_group`, `transaction_id`). If headers are detected, `SKIP_HEADER=1` is set for the COPY INTO command.

### Stage 4: Load to Bronze

Files are uploaded to a Snowflake internal stage (`@BRONZE.STG_INBOX`) via PUT, then loaded into Bronze tables via COPY INTO. Metadata columns (`batch_date`, `file_name`, `load_ts`) are added during load. `ON_ERROR = 'CONTINUE'` ensures partial files still load valid rows.

**Design consideration:** The sales table includes 7 data columns (including `source_id`) to handle the discrepancy between the column spec (6 columns) and the sample data (7 columns). `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE` in the file format allows files with either 6 or 7 columns to load.

### Stage 5: Silver Transform (dbt)

dbt incremental models transform Bronze to Silver:

- **silver_dim_store**: SCD Type 1 — upserts by `store_token`, keeps latest `store_name`/`store_group`, preserves `first_seen_ts`
- **silver_fact_sales**: Validates data types (timestamp, amount), filters invalid rows, deduplicates by `(store_token, transaction_id)` keeping the row with the latest `load_ts`
- **silver_sales_rejected**: Captures invalid rows with a `reject_reason` column for audit

**Incremental strategy:** All Silver models use `merge` strategy. On subsequent runs, only rows with `load_ts` greater than the current max are processed, making the pipeline efficient at scale.

### Stage 6: Gold Reports (dbt)

Three report tables are materialized as full tables (rebuilt each run):

- **Output 1** (`gold_output1_batch_report`): Raw/valid/invalid counts per batch date, limited to last 40 batch dates
- **Output 2** (`gold_output2_tx_date_report`): Daily sales stats with month-to-date accumulation and top store, limited to last 40 transaction dates
- **Output 3** (`gold_output3_top5_by_date`): Top 5 stores ranked by daily sales, last 10 transaction dates

### Stage 7: Archive

After successful ingestion, source CSV files are moved to `archive/{type}/{batch_date}/` for historical reference.

## Scalability Considerations

- **Bronze COPY INTO** parallelizes automatically in Snowflake based on file size
- **Silver incremental merge** avoids full reprocessing — only new data is transformed
- **Dedup via QUALIFY ROW_NUMBER()** is efficient in Snowflake's columnar engine
- **Adding new data sources** follows the same pattern: new Bronze table + Silver model
- **Warehouse sizing** is configurable — scale up the warehouse for larger loads without code changes
