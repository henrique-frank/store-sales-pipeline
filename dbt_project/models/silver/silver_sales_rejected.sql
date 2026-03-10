{{ config(
    materialized='incremental',
    unique_key=['store_token', 'transaction_id', 'load_ts']
) }}

with src as (
    select
        store_token,
        transaction_id,
        receipt_token,
        transaction_time as transaction_time_raw,
        amount as amount_raw,
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
)

select
    store_token,
    transaction_id,
    receipt_token,
    transaction_time_raw,
    amount_raw,
    user_role,
    batch_date,
    file_name,
    load_ts,
    case
        when store_token is null then 'NULL_STORE_TOKEN'
        when transaction_id is null then 'NULL_TRANSACTION_ID'
        when receipt_token is null then 'NULL_RECEIPT_TOKEN'
        when parsed_time is null then 'INVALID_TIMESTAMP'
        when parsed_amount is null then 'INVALID_AMOUNT'
        when length(receipt_token) < 5 or length(receipt_token) > 30 then 'INVALID_RECEIPT_TOKEN_LENGTH'
        else 'UNKNOWN'
    end as reject_reason
from src
where store_token is null
   or transaction_id is null
   or receipt_token is null
   or parsed_time is null
   or parsed_amount is null
   or length(receipt_token) < 5
   or length(receipt_token) > 30
