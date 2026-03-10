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
    qualify dense_rank() over (order by transaction_date desc) <= 10
),

ranked as (
    select
        b.transaction_date,
        b.store_token,
        sum(b.amount) as store_total_sales,
        row_number() over (
            partition by b.transaction_date
            order by sum(b.amount) desc
        ) as top_rank_id
    from base b
    inner join dates d on d.transaction_date = b.transaction_date
    group by 1, 2
)

select
    current_date() as snapshot_date,
    r.transaction_date,
    r.top_rank_id,
    r.store_total_sales,
    r.store_token,
    s.store_name
from ranked r
left join {{ ref('silver_dim_store') }} s
    on s.store_token = r.store_token
where r.top_rank_id <= 5
order by r.transaction_date desc, r.top_rank_id asc
