{{ config(
    materialized='incremental',
    unique_key=['store_token', 'transaction_id', 'load_ts']
) }}

with raw as (
    select
        store_token,
        transaction_id,
        receipt_token,
        transaction_time,
        amount,
        user_role,
        batch_date,
        file_name,
        load_ts,
        coalesce(
            try_to_timestamp_ntz(transaction_time),
            try_to_timestamp_ntz(transaction_time, 'YYYYMMDD"T"HH24MISS.FF3')
        ) as parsed_time,
        {{ clean_amount('amount') }} as parsed_amount
    from {{ source('bronze', 'sales_raw') }}
    {% if is_incremental() %}
    where load_ts > (select coalesce(max(load_ts), '1970-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
),

valid as (
    select *
    from raw
    where store_token is not null
      and transaction_id is not null
      and receipt_token is not null
      and parsed_time is not null
      and parsed_amount is not null
      and length(receipt_token) between 5 and 30
)

select
    store_token,
    transaction_id,
    receipt_token,
    parsed_time as transaction_time,
    parsed_amount as amount,
    user_role,
    batch_date,
    file_name,
    load_ts
from valid

