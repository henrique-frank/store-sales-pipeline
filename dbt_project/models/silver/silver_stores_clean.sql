{{ config(
    materialized='incremental',
    unique_key=['store_token', 'load_ts']
) }}

with raw as (
    select
        store_group,
        store_token,
        store_name,
        batch_date,
        load_ts
    from {{ source('bronze', 'stores_raw') }}
    {% if is_incremental() %}
    where load_ts > (select coalesce(max(load_ts), '1970-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
),

valid as (
    select *
    from raw
    where store_group is not null
      and store_token is not null
      and store_name is not null
)

select *
from valid

