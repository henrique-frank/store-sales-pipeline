{{ config(materialized='table') }}

with fact_by_day as (
    select
        transaction_time::date as transaction_date,
        store_token,
        amount
    from {{ ref('gold_fact_sales') }}
),

last_10_days as (
    -- Last 10 distinct transaction dates present in the fact table
    select distinct transaction_date
    from fact_by_day
    qualify dense_rank() over (order by transaction_date desc) <= 10
),

store_totals_per_day as (
    -- Total sales per store per day
    select
        f.transaction_date,
        f.store_token,
        sum(f.amount) as store_total_sales,
        row_number() over (
            partition by f.transaction_date
            order by sum(f.amount) desc
        ) as top_rank_id
    from fact_by_day f
    inner join last_10_days d
        on d.transaction_date = f.transaction_date
    group by 1, 2
)

select
    current_date() as snapshot_date,
    t.transaction_date,
    t.top_rank_id,
    t.store_total_sales,
    t.store_token,
    s.store_name
from store_totals_per_day t
left join {{ ref('gold_dim_store') }} s
    on s.store_token = t.store_token
   and s.is_current = true
where t.top_rank_id <= 5
order by t.transaction_date desc, t.top_rank_id asc

