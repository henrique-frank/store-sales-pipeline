# System Design — Store Sales Pipeline

## Architecture Overview

The pipeline follows a **Bronze / Silver / Gold** medallion architecture on Snowflake, with Python handling ingestion and dbt Core handling all transformations.

```mermaid
flowchart LR
  subgraph ingestion ["Python Ingestion"]
    inbox["inbox/ CSVs"] --> stage["PUT to Stage"]
    stage --> copy["COPY INTO"]
    copy --> archive["archive/"]
  end

  subgraph snowflake ["Snowflake — SALES_DW"]
    subgraph bronze ["Bronze (raw)"]
      storesRaw[stores_raw]
      salesRaw[sales_raw]
      logTable[ingestion_log]
    end
    subgraph silver ["Silver (clean + dedup)"]
      dimStore[dim_store]
      factSales[fact_sales]
      rejected[sales_rejected]
    end
    subgraph gold ["Gold (reports)"]
      out1["output1: batch report"]
      out2["output2: tx date stats"]
      out3["output3: top 5 stores"]
    end
  end

  copy --> storesRaw
  copy --> salesRaw
  copy --> logTable
  storesRaw -->|"dbt merge"| dimStore
  salesRaw -->|"dbt merge"| factSales
  salesRaw -->|"dbt incremental"| rejected
  factSales --> out1
  factSales --> out2
  factSales --> out3
  dimStore --> out3
```

### Pipeline Flow

```mermaid
flowchart TD
  A["Discover files in inbox/"] --> B{"File already processed?"}
  B -->|"Yes (hash match)"| C[Skip file]
  B -->|No| D["Detect header row"]
  D --> E["PUT file to @STG_INBOX"]
  E --> F["COPY INTO Bronze table"]
  F --> G["Log to ingestion_log"]
  G --> H["Move file to archive/"]
  H --> I["dbt run: Silver models"]
  I --> J["dbt run: Gold reports"]
  J --> K["dbt test: 27 data tests"]
```

## How This Design Meets the Assessment Requirements

- **Daily files & data sharing method**
  - As the PDF states, every day the partner uploads **zero or more** files into a bucket: one type with **stores’ data** and another with **daily transactions**. For development we treat this bucket as a local `inbox/` folder.
  - Python ingestion scans `inbox/`, detects file type by name (`stores_<batch_date>.csv` vs `sales_<batch_date>.csv`), extracts `batch_date` from the filename, and loads everything into **Bronze** (`BRONZE.STORES_RAW`, `BRONZE.SALES_RAW`) — see `ingestion/ingest.py`.
  - After processing the daily files, they are moved to `archive/{type}/{batch_date}/...`, which is the “different location where we store the historical information” mentioned in the spec.

- **File structure, validation, and “latest received” semantics**
  - Stores: enforce `store_group` (8‑char hex, uppercase), `store_token` (UUID, lowercase), `store_name` (< 200 chars).
  - Sales: enforce the 6 spec columns (`store_token`, `transaction_id`, `receipt_token`, `transaction_time`, `amount`, `user_role`), with flexible
    timestamp parsing (`transaction_time` independent from `batch_date`) and amount normalization from `$NN.NN` to `DECIMAL(11,2)`.
  - Duplicates: `silver_fact_sales` deduplicates on `(store_token, transaction_id)` using `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY load_ts DESC)
    = 1`, so we always **keep the latest received value**, as the spec requires.

- **Outputs 1, 2, and 3 (with limits)**
  - **Output 1 – by batch_date**: `gold_output1_batch_report` computes raw / valid / invalid counts per `batch_date` from Bronze + rejected tables,
    plus processing_date (`MAX(load_ts)::DATE`), and keeps only the **latest 40 batch dates** using `DENSE_RANK()`.
  - **Output 2 – by transaction_date**: `gold_output2_tx_date_report` aggregates by `transaction_time::DATE`, computes count of stores, total and
    average amount, and a **month‑to‑date running total** over the full month (not just the 40‑day window), and adds a **bonus** `TOP_STORE_TOKEN` for one
    of the top‑revenue stores. Limited to the **last 40 transaction dates**.
  - **Output 3 – top 5 per date**: `gold_output3_top5_by_date` ranks stores by daily sales using `ROW_NUMBER()`, keeps ranks 1–5 per date, joins
    `silver_dim_store` for `store_name`, and limits to the **last 10 transaction dates**, ensuring each date appears at most 5 times.

- **Questions log, assumptions, and data model (Tasks a/b)**
  - **Questions & answers** are logged in [`docs/questions.md`](questions.md) (e.g., 6 vs 7 sales columns, month‑accumulated semantics, documentation format).
  - **Assumptions** that unblock implementation are documented in [`docs/assumptions.md`](assumptions.md) (dedup semantics, formats, retention, multiple files per day,
    etc.).
  - The **logical data model** and relationships (Bronze → Silver → Gold, SCD2 `dim_store`, dedup strategy) are documented in [`docs/data_model.md`](data_model.md),
    and full Snowflake DDL lives in [`snowflake/setup.sql`](../snowflake/setup.sql).

- **Configuration and execution (Tasks c/d, technology constraints)**
  - System configuration is centralized in [`config/config.yaml.example`](../config/config.yaml.example) and [`dbt_project/profiles.yml.example`](../dbt_project/profiles.yml.example),
    with Snowflake credentials read from environment variables, and configurable paths (`inbox`, `archive`) and retention limits (40/40/10).
  - Execution instructions (install, configure, ingest, run dbt, and query outputs) are documented in [`README.md`](../README.md) and the interview
    [`docs/demo_guide.md`](demo_guide.md).
  - The implementation uses **Python** for ingestion and **Snowflake + dbt Core** for storage/processing, and the repo is structured and documented
    for easy sharing with the reviewer on GitHub.

### Technology Choices (Python + Snowflake/dbt)

- **Why Python**: The assessment explicitly calls out “use a programming language, preferably Python”. Python is used only where it adds the most value: file system access, config handling, and orchestrating `PUT` + `COPY INTO`. All data‑intensive work (validation, dedup, aggregations) is pushed down to Snowflake as SQL, so there are **no Python loops over data rows**, just loops over files.
- **Why Snowflake + dbt instead of Postgres**: Any database would satisfy “use of a database (e.g. Postgres or other)”, but Snowflake aligns better with the “millions of daily transactions” requirement: elastic warehouses, fast `COPY INTO`, and window functions at scale. dbt Core adds versioned, testable SQL models (Bronze → Silver → Gold) and incremental `merge` semantics, making the pipeline easy to extend (new sources, new outputs) without changing the ingestion code.

### Why SCD Type 2 for Stores

- The spec says store files are **additive**, but in practice store attributes (especially `store_name` and `store_group`) can change over time.
- Using **SCD Type 2** on `silver_dim_store` lets us keep a full history of those changes (with `valid_from`/`valid_to` and `is_current`) while still joining to the latest values in reports (`is_current = true`).
- This gives the best of both worlds for the assessment: a simple current‑state view for Outputs 2/3, plus the ability to answer historical questions later (e.g. “what was the name/group of this store when the transaction happened?”) without redesigning the model.

## Processing Flow

### Stage 1: Discover

The Python ingestion script scans the configured `inbox/` directory for files matching `stores_<YYYYMMDD>.csv` or `sales_<YYYYMMDD>.csv`. The batch date is extracted from the filename. Files not matching either pattern are ignored.

### Stage 2: Idempotency Check

Before processing, a SHA-256 content hash is computed for each file. The hash is checked against `BRONZE.INGESTION_LOG`. If already present, the file is skipped. This prevents duplicate processing when files are re-delivered or the pipeline is re-run.

### Stage 3: Header Detection

CSV files may or may not contain a header row. The ingestion script reads the first line and checks if it contains known column names (e.g., `store_group`, `transaction_id`). If headers are detected, `SKIP_HEADER=1` is set for the COPY INTO command.

### Stage 4: Load to Bronze

Files are uploaded to a Snowflake internal stage (`@BRONZE.STG_INBOX`) via PUT, then loaded into Bronze tables via COPY INTO. Metadata columns (`batch_date`, `file_name`, `load_ts`) are added during load. `ON_ERROR = 'CONTINUE'` ensures partial files still load valid rows.

**Design consideration:** Sales files contain 6 data columns as defined in the spec (store_token, transaction_id, receipt_token, transaction_time, amount, user_role). `ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE` in the file format provides resilience against unexpected extra fields.

### Stage 5: Silver Transform (dbt)

dbt incremental models transform Bronze to Silver:

- **silver_dim_store**: SCD Type 2 — tracks attribute changes over time. When `store_name` or `store_group` changes, the old record is closed (`is_current=false`, `valid_to` set) and a new current record is inserted. Downstream joins filter on `is_current=true` for the latest values.
- **silver_fact_sales**: Validates data types (timestamp parsed with explicit format `YYYYMMDDTHH24MISS.FF3` plus standard formats, amount stripped of `$`), filters invalid rows, deduplicates by `(store_token, transaction_id)` keeping the row with the latest `load_ts`
- **silver_sales_rejected**: Captures invalid rows with a `reject_reason` column for audit

**Incremental strategy:** All Silver models use `merge` strategy. On subsequent runs, only rows with `load_ts` greater than the current max are processed, making the pipeline efficient at scale.

### Stage 6: Gold Reports (dbt)

Three report tables are materialized as full tables (rebuilt each run):

- **Output 1** (`gold_output1_batch_report`): Raw/valid/invalid counts per batch date, limited to last 40 batch dates
- **Output 2** (`gold_output2_tx_date_report`): Daily sales stats with month-to-date accumulation and top store, limited to last 40 transaction dates
- **Output 3** (`gold_output3_top5_by_date`): Top 5 stores ranked by daily sales, last 10 transaction dates

### Stage 7: Archive

After successful ingestion, source CSV files are moved to `archive/{type}/{batch_date}/` for historical reference.

## Multiple Files per Batch Date

The spec states "one or more files" per day, so the system is designed to handle multiple sales files sharing the same `batch_date`:

- **Ingestion**: `glob("*.csv")` picks up all files in the inbox. Each is processed independently.
- **Idempotency**: Based on SHA-256 content hash, not filename. Two files named `sales_20211001.csv` with different content both get ingested. Exact duplicates are skipped.
- **Bronze**: All files land in `SALES_RAW` with the same `batch_date` but different `file_name` / `load_ts`.
- **Silver dedup**: `QUALIFY ROW_NUMBER() OVER (PARTITION BY store_token, transaction_id ORDER BY load_ts DESC) = 1` keeps the latest received across all files.
- **Archive**: If the same filename already exists in the archive (from a prior run), the file is saved with a hash suffix (e.g., `sales_20211001_a3f8b2c1.csv`) to prevent overwriting.
- **Batch date extraction**: The regex extracts the first `YYYYMMDD` pattern found in the filename, so formats like `sales_20211001_2.csv` are also supported.

## Key Considerations from Spec

1. **`transaction_time` ≠ `batch_date`**: These are independent concepts. `batch_date` (from the filename) tracks when the file was delivered; `transaction_time` (from the row data) is when the sale actually occurred. Silver and Gold use `transaction_time` for analytics, while `batch_date` drives Output 1 (ingestion tracking).

2. **Dedup key = `(store_token, transaction_id)`**: As stated in the spec, a unique transaction is identified by this combination. `silver_fact_sales` applies `QUALIFY ROW_NUMBER() OVER (PARTITION BY store_token, transaction_id ORDER BY load_ts DESC) = 1` to enforce this.

3. **Duplicated transactions, latest received wins**: The spec states that duplicated transactions may happen and that a unique transaction is the combination of `store_token` and `transaction_id`, keeping the **latest received value**. In `silver_fact_sales` we implement this by partitioning on `(store_token, transaction_id)` and ordering by `load_ts DESC`, so only the row with the most recent ingestion timestamp is kept in Silver; earlier duplicates are overwritten by the incremental merge.

## Scalability — Designed for Millions of Daily Transactions

Although the sample data is small, this pipeline is designed to handle production volumes of millions of daily transactions without architectural changes.

| Concern | How it scales |
|---------|---------------|
| **Ingestion throughput** | Snowflake COPY INTO parallelizes automatically by file. Split large files into partitioned CSVs for maximum throughput. |
| **Transform efficiency** | Silver models use incremental merge — only rows with new `load_ts` are processed. Cost stays constant regardless of history size. |
| **Dedup performance** | `QUALIFY ROW_NUMBER()` runs in a single pass over Snowflake's columnar engine. No self-join or temp tables. |
| **Warehouse sizing** | Scale from X-Small to 4XL without code changes. Double the warehouse size = roughly half the runtime. |
| **Query performance** | At production scale, apply `CLUSTER BY (store_token, transaction_time::date)` on `silver_fact_sales` to optimize scan pruning. |
| **Near-real-time** | Replace PUT + COPY with Snowpipe for continuous ingestion. Same Bronze tables, no downstream changes. |
| **New data sources** | Follow the same pattern: new Bronze raw table + Silver dbt model. Gold reports can reference new sources seamlessly. |
| **Orchestration** | Wrap Python ingestion + dbt in Airflow, Snowflake Tasks, or any scheduler. The pipeline is stateless and idempotent. |

In practice, the project was also tested with a **multi‑day synthetic dataset** that includes:

- Many batch dates and multiple files for the same day (e.g. `sales_YYYYMMDD.csv` + `sales_YYYYMMDD_2.csv`) to exercise the 40/40/10 retention windows and dedup across files.
- Random duplicates and invalid rows to validate that Silver and `sales_rejected` scale with more volume without changing any code.

The key scalability decisions are:

- **All heavy lifting in SQL** (window functions, aggregates, joins) rather than Python loops, so performance scales with Snowflake’s warehouse size, not the Python process.
- **Incremental models** in Silver avoid reprocessing history; only new `load_ts` slices are transformed on each run.
- **Idempotent file processing** via SHA‑256 hashes keeps ingestion simple even if upstream re‑delivers large files.
