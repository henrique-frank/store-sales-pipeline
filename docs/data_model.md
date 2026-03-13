# Data Model — Store Sales Pipeline

## Logical Model

```mermaid
erDiagram
  BRONZE_STORES_RAW {
    string store_group
    string store_token
    string store_name
    date   batch_date
    string file_name
    timestamp load_ts
  }

  BRONZE_SALES_RAW {
    string store_token
    string transaction_id
    string receipt_token
    string transaction_time
    string amount
    string user_role
    date   batch_date
    string file_name
    timestamp load_ts
  }

  BRONZE_INGESTION_LOG {
    int    file_id
    string file_type
    date   batch_date
    string file_name
    string content_hash  -- stores file_name; kept for backwards-compatibility
    int    row_count
    string status
    timestamp loaded_at
  }

  SILVER_SALES_REJECTED {
    string store_token
    string transaction_id
    string receipt_token
    string transaction_time
    string amount
    string user_role
    date   batch_date
    string file_name
    string reject_reason
    timestamp load_ts
  }

  REPORT_OUTPUT1_BATCH_REPORT {
    date snapshot_date
    date batch_date
    int  total_processed_raw
    int  total_valid
    int  total_invalid
    date processing_date
  }

  REPORT_OUTPUT2_TX_DATE_REPORT {
    date snapshot_date
    date transaction_date
    int  stores_with_tx
    number total_sales_amount
    number total_sales_avg
    number month_accumulated_sales
    string top_store_token
  }

  REPORT_OUTPUT3_TOP5_BY_DATE {
    date snapshot_date
    date transaction_date
    int  top_rank_id
    number store_total_sales
    string store_token
    string store_name
  }

  BRONZE_STORES_RAW ||--o{ GOLD_DIM_STORE : "clean + SCD2"
  BRONZE_SALES_RAW  ||--o{ GOLD_FACT_SALES : "clean + dedup"
  BRONZE_SALES_RAW  ||--o{ SILVER_SALES_REJECTED : "invalid rows"
  BRONZE_INGESTION_LOG ||--o{ REPORT_OUTPUT1_BATCH_REPORT : "file metrics"

  GOLD_DIM_STORE    ||--o{ REPORT_OUTPUT3_TOP5_BY_DATE : "lookup store_name"
  GOLD_FACT_SALES   ||--o{ REPORT_OUTPUT1_BATCH_REPORT : "valid counts"
  GOLD_FACT_SALES   ||--o{ REPORT_OUTPUT2_TX_DATE_REPORT : "tx date stats"
  GOLD_FACT_SALES   ||--o{ REPORT_OUTPUT3_TOP5_BY_DATE : "daily store totals"
```

## Key Relationships

- `fact_sales.store_token` references `dim_store.store_token`
- `output3.store_token` joins `dim_store` for `store_name`
- `output1` uses Bronze `sales_raw` for raw counts + `sales_rejected` for invalid counts

## Deduplication Strategy

```sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY store_token, transaction_id
    ORDER BY load_ts DESC
) = 1
```

When the same `(store_token, transaction_id)` pair appears multiple times (across files or within the same file), only the row with the latest `load_ts` is kept in `fact_sales`. Previous versions are overwritten via the incremental merge strategy.

## DDL Reference

Full DDL is in [`snowflake/setup.sql`](../snowflake/setup.sql).

### Bronze Tables

| Table | Purpose |
|-------|---------|
| `BRONZE.STORES_RAW` | Raw store attributes as received |
| `BRONZE.SALES_RAW` | Raw sales transactions as received |
| `BRONZE.INGESTION_LOG` | File processing audit + idempotency |

### Silver Tables (managed by dbt)

| Table | Strategy | Key |
|-------|----------|-----|
| `SILVER.SILVER_SALES_REJECTED` | Incremental | `(store_token, transaction_id, load_ts)` |

### Gold Tables (managed by dbt)

| Table | Materialization | Rows |
|-------|----------------|------|
| `GOLD.GOLD_DIM_STORE` | Incremental SCD2 dimension | n/a |
| `GOLD.GOLD_FACT_SALES` | Incremental deduped fact | n/a |

### Report Tables (managed by dbt)

| Table | Materialization | Rows |
|-------|----------------|------|
| `REPORTS.REPORT_OUTPUT1_BATCH_REPORT` | Table (rebuilt) | <= 40 |
| `REPORTS.REPORT_OUTPUT2_TX_DATE_REPORT` | Table (rebuilt) | <= 40 |
| `REPORTS.REPORT_OUTPUT3_TOP5_BY_DATE` | Table (rebuilt) | <= 50 |
