# Demo Guide — 1h Technical Interview

## Pre-Meeting Setup (5 min before)

Open **3 windows**:
1. **Terminal** — in `C:\code\r2-assignement`
2. **Snowflake UI** — `https://app.snowflake.com` → SALES_DW
3. **VS Code / Cursor** — project open to walk through code

Set env vars in terminal:
```powershell
$env:SNOWFLAKE_ACCOUNT = "jdpzoyh-vo22700"
$env:SNOWFLAKE_USER = "HENRIQUEFRANK"
$env:SNOWFLAKE_PASSWORD = "<your_password>"
```

Clean Snowflake for fresh demo:
```powershell
python -c "
import snowflake.connector, os
conn = snowflake.connector.connect(account=os.environ['SNOWFLAKE_ACCOUNT'], user=os.environ['SNOWFLAKE_USER'], password=os.environ['SNOWFLAKE_PASSWORD'], database='SALES_DW', warehouse='COMPUTE_WH', role='ACCOUNTADMIN')
cur = conn.cursor()
for t in ['BRONZE.SALES_RAW','BRONZE.STORES_RAW','BRONZE.INGESTION_LOG']: cur.execute(f'TRUNCATE TABLE {t}')
cur.execute('REMOVE @BRONZE.STG_INBOX')
print('Ready')
conn.close()
"
```

---

## Part 1 — Architecture Walkthrough (10 min)

**Show `README.md` diagram:**

```
  inbox/ (CSVs)  →  BRONZE (raw)  →  SILVER (clean+dedup)  →  GOLD (reports)
                    Python PUT+COPY     dbt incremental merge    dbt table
```

**Key points to mention:**
- Python handles ingestion only (no data transformation in Python)
- dbt handles all transformation (Bronze → Silver → Gold)
- Incremental merge — doesn't reprocess old data
- Idempotency via SHA-256 content hash
- No for-loops over data — everything is SQL-based, scales with warehouse size

**Show project structure briefly** (in editor):
- `ingestion/` → 3 files (ingest, config, validate)
- `dbt_project/models/silver/` → dim_store, fact_sales, sales_rejected
- `dbt_project/models/gold/` → output1, output2, output3
- `docs/` → design, assumptions, data_model, questions

---

## Part 2 — Live Demo with Light Data (15 min)

### Load files
```powershell
mkdir inbox -ErrorAction SilentlyContinue
Copy-Item data\light\*.csv inbox\
python -m ingestion.ingest --config config/config.yaml
```

**Expected output:**
```
Found 6 file(s) to process.
  sales_20241001.csv: LOADED (16 rows)
  sales_20241002.csv: LOADED (16 rows)
  sales_20241003.csv: LOADED (16 rows)
  stores_20241001.csv: LOADED (10 rows)
  stores_20241002.csv: LOADED (10 rows)
  stores_20241003.csv: LOADED (10 rows)
```

### Show idempotency
```powershell
Copy-Item data\light\*.csv inbox\
python -m ingestion.ingest --config config/config.yaml
```
**Expected:** All files show `SKIPPED` — same content hash already processed.

### Run dbt
```powershell
cd dbt_project
python -m dbt.cli.main build --profiles-dir .
cd ..
```
**Expected:** 6 models OK, 27 tests PASS.

### Query results (in Snowflake UI or terminal)

**Output 1 — Batch Report:**
```sql
SELECT * FROM GOLD.GOLD_OUTPUT1_BATCH_REPORT ORDER BY batch_date DESC;
```
> "Shows raw vs valid vs invalid counts per batch date. We count valid format before dedup — duplicates aren't invalid, just redundant."

**Output 2 — Sales by Transaction Date:**
```sql
SELECT * FROM GOLD.GOLD_OUTPUT2_TX_DATE_REPORT ORDER BY transaction_date DESC;
```
> "Month accumulated uses a window function over the full month, not just the 40-date window. Top store is a bonus column."

**Output 3 — Top 5 Stores:**
```sql
SELECT * FROM GOLD.GOLD_OUTPUT3_TOP5_BY_DATE ORDER BY transaction_date DESC, top_rank_id;
```
> "Ranked by daily sales. Joins dim_store for store_name. Max 5 per date, last 10 dates."

---

## Part 3 — Design Decisions (10 min)

**Be ready to explain these:**

| Decision | Why |
|---|---|
| Bronze/Silver/Gold | Separation of concerns — raw audit trail, clean data, reports |
| Incremental merge in Silver | Avoids full reprocessing. Only new `load_ts` rows are transformed |
| `QUALIFY ROW_NUMBER()` for dedup | Efficient, no self-join. Snowflake-native |
| `TRY_TO_TIMESTAMP_NTZ` with fallback format | Defensive parsing — handles standard + compact `20211001T174600.000` |
| Content hash idempotency | Simple, reliable. No need for watermarks or state management |
| `ON_ERROR = 'CONTINUE'` | Partial loads succeed. Invalid rows go to `sales_rejected` |
| 6 columns per spec | Confirmed by Ricardo — source_id from sample is ignored |
| Gold as full table rebuild | Small tables (<50 rows). Fast, simple, always consistent |
| Config via env vars | No credentials in code. Same image works in any environment |

**If asked "how would you scale this?":**
- Warehouse size up (no code change)
- `CLUSTER BY (store_token, transaction_time::date)` on fact_sales
- Split large files into partitioned CSVs for parallel COPY INTO
- Add Snowpipe for near-real-time ingestion (same Bronze tables)
- New data source = new Bronze table + new Silver model, Gold references unified

---

## Part 4 — Scalability Demo (5 min, if asked)

```powershell
python data\generate_samples.py
Copy-Item data\heavy\*.csv inbox\
python -m ingestion.ingest --config config/config.yaml
cd dbt_project
python -m dbt.cli.main build --profiles-dir . --full-refresh
cd ..
```

**Talking points:**
- 100k rows, 500 stores, 30 days
- Ingestion: ~60 seconds (60 files)
- dbt build: ~7 seconds (Silver + Gold + 27 tests)
- Same code, no config changes, just more data

---

## Part 5 — Pair Programming / Extension (15 min)

**Likely asks and how to handle:**

| They might ask | How to approach |
|---|---|
| "Add a new column to sales" | Add to Bronze DDL, update COPY INTO, add to Silver model |
| "Add a new data source" | New Bronze table + Silver model, same pattern |
| "Change dedup logic" | Update the `ORDER BY` in `QUALIFY ROW_NUMBER()` |
| "Add data quality checks" | Add dbt tests in `schema.yml` or custom test macros |
| "Export outputs as CSV" | Add `dbt run-operation` macro or Python export script |
| "Schedule this daily" | Snowflake Task, Airflow DAG, or cron wrapping Python + dbt |

---

## Ricardo's Key Interests — Talking Points

Based on your initial call, these are things he specifically values:

### "No giant for loops" — Scalable by design
> "Our Python code has zero loops over data rows. It only loops over files in the inbox (file I/O). All data transformation is pure SQL in dbt — Snowflake handles the parallelism. Going from 50 rows to 100k rows required zero code changes, just more data."

### "Factory of processes" — Repeatable pattern
> "Adding a new data source follows the exact same pattern: (1) add a Bronze raw table in setup.sql, (2) add a COPY INTO in ingest.py, (3) create a Silver dbt model to clean and dedup, (4) Gold reports reference the new Silver table. No new framework, no new abstraction — just repeat the pattern."

### "How you document and explain"
> Walk through `docs/` briefly. Mention the questions log shows initiative — you asked before building. Assumptions doc shows you didn't block on unknowns. Design doc shows intentional architecture, not just code that works.

### "You can use AI, but the solution is yours"
**Be ready to explain without hesitation:**
- Why `QUALIFY ROW_NUMBER() OVER (... ORDER BY load_ts DESC)` and not a subquery or self-join?
  → Snowflake-native, single pass, no temp table
- Why incremental merge and not full refresh in Silver?
  → Avoids reprocessing history. Only new `load_ts` rows are transformed. Scales linearly.
- Why content hash for idempotency and not filename?
  → Same filename could have different content (re-delivery with corrections). Hash catches actual content changes.
- Why `TRY_TO_TIMESTAMP_NTZ` with fallback format?
  → Default parser doesn't handle compact `20211001T174600.000`. Fallback is defensive — handles both formats.
- Why count invalid from `sales_rejected` instead of `raw - fact_sales`?
  → `fact_sales` is deduplicated. Duplicate valid rows aren't invalid — they're just redundant. Counting from rejected gives the true invalid count.
- Why `ON_ERROR = 'CONTINUE'`?
  → Partial loads succeed. One bad row doesn't block 3,000 good rows. Bad rows are caught in Silver validation.
- Why Gold tables are full rebuild, not incremental?
  → Max 40-50 rows. Full rebuild is faster and simpler than tracking what changed. Consistency guaranteed.

### "Challenge the solution" — Known trade-offs to acknowledge
- **No orchestrator**: Pipeline is run manually (Python then dbt). For production, would wrap in Airflow or Snowflake Tasks.
- **No streaming**: Batch-only. For near-real-time, Snowpipe replaces the PUT+COPY step, same Bronze tables.
- **No column-level validation in Bronze**: We load everything raw and validate in Silver. This is intentional — Bronze is the audit trail.
- **Idempotency gap**: If COPY INTO succeeds but log INSERT fails (crash between the two), the file could be double-loaded. Fix: wrap in a transaction or add a staging step. For this volume, the risk is minimal.

---

## Quick Reference — Key Files

| File | What it does |
|---|---|
| `ingestion/ingest.py` | Scan inbox, PUT, COPY INTO, archive |
| `ingestion/validate.py` | File type, batch date, header detection, content hash |
| `dbt_project/models/silver/silver_fact_sales.sql` | Validate + dedup (the core logic) |
| `dbt_project/models/gold/gold_output2_tx_date_report.sql` | Most complex report (month accumulation + top store) |
| `dbt_project/macros/clean_amount.sql` | Strip `$` from amounts |
| `snowflake/setup.sql` | Full DDL |
| `docs/assumptions.md` | Your documented decisions |
