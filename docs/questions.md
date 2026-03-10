# Questions Log — Store Sales Pipeline

## Sent to Product Team

Status: **Pending response** | Sent: 2026-03-10

1. **Sales file column mismatch**: The spec defines 6 columns (store_token, transaction_id, receipt_token, transaction_time, amount, user_role), but the transposed sample includes a `source_id` field between `amount` and `user_role` (7 fields). Should we expect this extra column in the actual files?

2. **Output 2 — month accumulated sales**: "Total accumulated sales amount for the transaction's month" — is this a month-to-date running total (cumulative up to that transaction date) or the full month total repeated on each row?

## Additional Questions (documented, proceeding with assumptions)

3. **Output format**: Task (c) mentions "output formats" as configurable. Should the 3 outputs be exported as CSV files, kept as database tables, or both?
   - *Assumption*: Tables in Snowflake + configurable CSV export to `out/` folder.

4. **Multiple files per batch_date**: "Zero or more files per day" — can two files with the same name (e.g. two `sales_20211001.csv`) arrive on the same day? Does the second overwrite or append?
   - *Assumption*: Multiple files per type/batch_date are possible. We union all rows and deduplicate downstream. Idempotency via content hash prevents reprocessing the same file.

5. **Store name changes**: Can `store_name` change for an existing `store_token` across different batch dates?
   - *Assumption*: Yes, possible. We apply SCD Type 1 (overwrite with latest received, keep `first_seen_ts`).

6. **Amount currency/format**: The spec says `Numeric(11,2)` but the sample shows `$63.98` with a dollar sign. Is the `$` prefix always present? Can other currencies appear?
   - *Assumption*: Always USD with `$` prefix. We strip `$` and parse to decimal. Unparseable values are marked invalid.

7. **transaction_time format**: The sample shows `20211001T174600.000`. Is this the only expected format, or should we handle multiple timestamp formats (ISO 8601, etc.)?
   - *Assumption*: Accept any format parseable by Snowflake's `TRY_TO_TIMESTAMP_NTZ()`. Unparseable values are marked invalid.

8. **Dedup "latest received"**: Confirmed from spec — "keep the latest received value" means by ingestion/load timestamp (order of arrival in our system), not by `transaction_time`.

9. **Retention / purging**: Should older data beyond the output windows (40/40/10) be purged from the database, or only filtered in reports?
   - *Assumption*: Keep all data in Silver. Retention limits only apply to Gold report outputs.
