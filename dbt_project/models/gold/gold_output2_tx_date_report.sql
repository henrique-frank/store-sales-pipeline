{{ config(materialized='table') }}

with base as (
    select
        transaction_time::date as transaction_date,
        store_token,
        amount
    from {{ ref('silver_fact_sales') }}
),

dates as (
    select distinct transaction_date
    from base
    qualify dense_rank() over (order by transaction_date desc) <= 40
),

daily as (
    select
        b.transaction_date,
        count(distinct b.store_token) as stores_with_tx,
        sum(b.amount) as total_sales_amount,
        avg(b.amount) as total_sales_avg
    from base b
    inner join dates d on d.transaction_date = b.transaction_date
    group by 1
),

month_accumulated as (
    select
        transaction_date,
        sum(total_sales_amount) over (
            partition by date_trunc('month', transaction_date)
            order by transaction_date
            rows between unbounded preceding and current row
        ) as month_accumulated_sales
    from daily
),

top_store as (
    select
        transaction_date,
        store_token as top_store_token
    from (
        select
            b.transaction_date,
            b.store_token,
            sum(b.amount) as store_total,
            row_number() over (
                partition by b.transaction_date
                order by sum(b.amount) desc
            ) as rn
        from base b
        inner join dates d on d.transaction_date = b.transaction_date
        group by 1, 2
    )
    where rn = 1
)

select
    current_date() as snapshot_date,
    d.transaction_date,
    day.stores_with_tx,
    day.total_sales_amount,
    round(day.total_sales_avg, 2) as total_sales_avg,
    ma.month_accumulated_sales,
    ts.top_store_token
from dates d
left join daily day on day.transaction_date = d.transaction_date
left join month_accumulated ma on ma.transaction_date = d.transaction_date
left join top_store ts on ts.transaction_date = d.transaction_date
order by d.transaction_date desc
