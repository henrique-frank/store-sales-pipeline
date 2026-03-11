# Data Model — Store Sales Pipeline

## Logical Model

```
BRONZE (raw)                    SILVER (clean)                  GOLD (reports)
┌─────────────────┐            ┌──────────────────┐
│ stores_raw      │───────────>│ dim_store (SCD2)  │──────────> output3_top5
│ store_group     │            │ dim_store_key (PK)│
│ store_token     │            │ store_token       │
│ store_name      │            │ store_group       │
│ batch_date      │            │ store_name        │
│ file_name       │            │ valid_from        │
│ load_ts         │            │ valid_to          │
└─────────────────┘            │ is_current        │
                               │ first_seen_ts     │
                               │ last_load_ts      │
                               └──────────────────┘

┌─────────────────┐            ┌──────────────────┐
│ sales_raw       │───────────>│ fact_sales        │──────────> output1_batch
│ store_token     │            │ store_token (PK)  │──────────> output2_tx_date
│ transaction_id  │            │ transaction_id(PK)│──────────> output3_top5
│ receipt_token   │            │ receipt_token     │
│ transaction_time│            │ transaction_time  │
│ amount          │            │ amount            │
│ user_role       │            │ user_role         │
│ batch_date      │            │ batch_date        │
│ file_name       │            │ last_load_ts      │
│ load_ts         │            └──────────────────┘
└─────────────────┘            ┌──────────────────┐
                               │ sales_rejected   │
┌─────────────────┐            │ (same as raw +   │
│ ingestion_log   │            │  reject_reason)  │
│ file_id (PK)    │            └──────────────────┘
│ file_type       │
│ batch_date      │
│ file_name       │
│ content_hash(UQ)│
│ row_count       │
│ status          │
│ loaded_at       │
└─────────────────┘
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
| `SILVER.SILVER_DIM_STORE` | Incremental merge | `store_token` |
| `SILVER.SILVER_FACT_SALES` | Incremental merge | `(store_token, transaction_id)` |
| `SILVER.SILVER_SALES_REJECTED` | Incremental | `(store_token, transaction_id, load_ts)` |

### Gold Tables (managed by dbt)

| Table | Materialization | Rows |
|-------|----------------|------|
| `GOLD.GOLD_OUTPUT1_BATCH_REPORT` | Table (rebuilt) | <= 40 |
| `GOLD.GOLD_OUTPUT2_TX_DATE_REPORT` | Table (rebuilt) | <= 40 |
| `GOLD.GOLD_OUTPUT3_TOP5_BY_DATE` | Table (rebuilt) | <= 50 |
