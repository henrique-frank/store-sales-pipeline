{{ config(
    materialized='incremental',
    unique_key=['store_token', 'transaction_id'],
    incremental_strategy='merge'
) }}

with src as (
    select
        store_token,
        transaction_id,
        receipt_token,
        coalesce(
            try_to_timestamp_ntz(transaction_time),
            try_to_timestamp_ntz(transaction_time, 'YYYYMMDD\"T\"HH24MISS.FF3')
        ) as transaction_time,
        {{ clean_amount('amount') }} as amount,
        user_role,
        batch_date,
        load_ts
    from {{ source('bronze', 'sales_raw') }}
),

valid as (
    select *
    from src
    where store_token is not null
      and transaction_id is not null
      and receipt_token is not null
      and transaction_time is not null
      and amount is not null
      and length(receipt_token) between 5 and 30
    {% if is_incremental() %}
      and load_ts > (select coalesce(max(last_load_ts), '1970-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
),

dedup as (
    select *
    from valid
    qualify row_number() over (
        partition by store_token, transaction_id
        order by load_ts desc
    ) = 1
)

select
    store_token,
    transaction_id,
    receipt_token,
    transaction_time,
    amount,
    user_role,
    batch_date,
    load_ts as last_load_ts
from dedup
