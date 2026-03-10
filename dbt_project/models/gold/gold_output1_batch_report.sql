{{ config(materialized='table') }}

with batch_dates as (
    select distinct batch_date
    from {{ source('bronze', 'sales_raw') }}
    qualify dense_rank() over (order by batch_date desc) <= 40
),

raw_counts as (
    select
        batch_date,
        count(*) as total_raw
    from {{ source('bronze', 'sales_raw') }}
    group by 1
),

invalid_counts as (
    select
        batch_date,
        count(*) as total_invalid
    from {{ ref('silver_sales_rejected') }}
    group by 1
),

processing_dates as (
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
    coalesce(r.total_raw, 0) - coalesce(i.total_invalid, 0) as total_valid,
    coalesce(i.total_invalid, 0) as total_invalid,
    coalesce(p.processing_date, current_date()) as processing_date
from batch_dates b
left join raw_counts r on r.batch_date = b.batch_date
left join invalid_counts i on i.batch_date = b.batch_date
left join processing_dates p on p.batch_date = b.batch_date
order by b.batch_date desc
