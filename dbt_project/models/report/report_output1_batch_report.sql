{{ config(materialized='table') }}

with batch_dates as (
    -- Last 40 distinct batch dates present in the raw sales data
    select distinct batch_date
    from {{ source('bronze', 'sales_raw') }}
    qualify dense_rank() over (order by batch_date desc) <= 40
),

raw_counts as (
    -- How many raw rows arrived per batch_date (before validation)
    select
        batch_date,
        count(*) as total_raw
    from {{ source('bronze', 'sales_raw') }}
    group by 1
),

valid_counts as (
  -- How many valid, deduplicated rows ended up in the fact table
  select
      batch_date,
      count(*) as total_valid
  from {{ ref('gold_fact_sales') }}
  group by 1
),

invalid_counts as (
    -- How many rows were rejected at the Silver validation step
    select
        batch_date,
        count(*) as total_invalid
    from {{ ref('silver_sales_rejected') }}
    group by 1
),

processing_dates as (
    -- When we last processed data for this batch_date
    select
        batch_date,
        max(load_ts)::date as processing_date
    from {{ source('bronze', 'sales_raw') }}
    group by 1
)

select
    current_date() as snapshot_date,
    b.batch_date,
    coalesce(r.total_raw, 0) as total_processed_raw,
    coalesce(v.total_valid, 0) as total_valid,
    coalesce(i.total_invalid, 0) as total_invalid,
    coalesce(p.processing_date, current_date()) as processing_date
from batch_dates b
left join raw_counts r on r.batch_date = b.batch_date
left join valid_counts v on v.batch_date = b.batch_date
left join invalid_counts i on i.batch_date = b.batch_date
left join processing_dates p on p.batch_date = b.batch_date
order by b.batch_date desc

