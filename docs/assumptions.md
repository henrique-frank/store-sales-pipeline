# Assumptions — Store Sales Pipeline

These assumptions were made to proceed with the first version. Items marked with a question were sent to the product team and may be revised based on their response.

## Data & Dedup

1. **"Keep the latest received value"** means latest by ingestion timestamp (`load_ts`), not by `transaction_time`. Rationale: "received" implies arrival order in our system.

2. **Dedup key** is `(store_token, transaction_id)` as stated in the spec.

3. **Dedup and SCD are applied in curated layers (not Bronze)**: Bronze is append-only and stores "what arrived, when it arrived" for audit and replay. Deduplication by `(store_token, transaction_id)` and SCD Type 2 for stores happen in the curated dim/fact tables (Silver/Gold), where business rules belong.

## File Format

4. **Amount always includes `$` prefix** (as shown in the sample: `$63.98`). We strip `$` and parse to `DECIMAL(11,2)`. Unparseable values are marked invalid.

5. **`source_id` column** (ANSWERED): The sample showed 7 columns including `source_id`, but Ricardo confirmed this should be ignored — work with the 6 columns from the spec only.

6. **Header detection**: Files with headers are auto-detected by checking if the first row contains known column names. `SKIP_HEADER` is set accordingly.

7. **`transaction_time` format**: Sample shows `20211001T174600.000`. Snowflake's default `TRY_TO_TIMESTAMP_NTZ()` doesn't parse this compact format automatically, so we apply an explicit format (`YYYYMMDD"T"HH24MISS.FF3`) as fallback. Both standard ISO and compact formats are accepted. Unparseable values are marked invalid.

## Output Definitions

8. **Output 2 "month accumulated sales"** (ANSWERED): Confirmed as month-to-date running total (cumulative sum up to each transaction date within the month).

9. **Output 1 counts**: `total_processed_raw` = all rows loaded into Bronze for that batch date. `total_invalid` = rows captured in `sales_rejected` (failed validation). `total_valid` = rows in the Gold fact table (`fact_sales`) for that `batch_date` (i.e., valid rows after applying the dedup rule).

10. **"Last 40/10 dates"**: Refers to distinct dates with data available, not calendar days.

## Operations

11. **Idempotency**: Files are tracked by `(file_type, batch_date, file_name)` in `BRONZE.INGESTION_LOG`. If a file with the same triple is already marked as `LOADED`, it is skipped on re-runs.

12. **Data retention**: All data is retained in Silver. Retention limits (40/40/10) only apply to Gold report outputs.

13. **Multiple files per batch_date**: The spec states "one or more files" per day, so multiple sales files can share the same `batch_date`. Each physical file is tracked independently by `(file_type, batch_date, file_name)`. All rows are loaded into Bronze and deduplication by `(store_token, transaction_id)` happens in the curated tables.

14. **Archive**: Processed files are moved to `archive/{ingestion_date}/{type}/{batch_date}/`. We assume upstream does not resend the exact same file name for the same batch date with different content; if it does, the later copy simply overwrites the prior one in the archive.
