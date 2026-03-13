{{ config(materialized='table') }}

with fact_by_day as (
    -- One row per transaction with a transaction_date
    select
        transaction_time::date as transaction_date,
        store_token,
        amount
    from {{ ref('gold_fact_sales') }}
),

last_40_days as (
    -- Last 40 distinct transaction dates present in the fact table
    select distinct transaction_date
    from fact_by_day
    qualify dense_rank() over (order by transaction_date desc) <= 40
),

daily_metrics as (
    -- Daily totals and averages across all stores
    select
        transaction_date,
        count(distinct store_token) as stores_with_tx,
        sum(amount) as total_sales_amount,
        avg(amount) as total_sales_avg
    from fact_by_day
    group by 1
),

monthly_running_totals as (
    -- Month-to-date accumulated sales amount per transaction_date
    select
        transaction_date,
        sum(total_sales_amount) over (
            partition by date_trunc('month', transaction_date)
            order by transaction_date
            rows between unbounded preceding and current row
        ) as month_accumulated_sales
    from daily_metrics
),

top_store_per_day as (
    -- For each day, pick the store with the highest total sales
    select
        transaction_date,
        store_token as top_store_token
    from (
        select
            f.transaction_date,
            f.store_token,
            sum(f.amount) as store_total,
            row_number() over (
                partition by f.transaction_date
                order by sum(f.amount) desc
            ) as rn
        from fact_by_day f
        inner join last_40_days d on d.transaction_date = f.transaction_date
        group by 1, 2
    )
    where rn = 1
)

select
    current_date() as snapshot_date,
    d.transaction_date,
    m.stores_with_tx,
    m.total_sales_amount,
    round(m.total_sales_avg, 2) as total_sales_avg,
    t.month_accumulated_sales,
    s.top_store_token
from last_40_days d
join daily_metrics m
  on m.transaction_date = d.transaction_date
left join monthly_running_totals t
  on t.transaction_date = d.transaction_date
left join top_store_per_day s
  on s.transaction_date = d.transaction_date
order by d.transaction_date desc

