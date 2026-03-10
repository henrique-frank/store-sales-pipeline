{{ config(
    materialized='incremental',
    unique_key='store_token',
    incremental_strategy='merge',
    merge_exclude_columns=['first_seen_ts']
) }}

with src as (
    select
        store_token,
        store_group,
        store_name,
        load_ts
    from {{ source('bronze', 'stores_raw') }}
    where store_token is not null
      and store_group is not null
      and store_name is not null
    {% if is_incremental() %}
      and load_ts > (select coalesce(max(last_load_ts), '1970-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
),

dedup as (
    select *
    from src
    qualify row_number() over (
        partition by store_token
        order by load_ts desc
    ) = 1
)

select
    store_token,
    store_group,
    store_name,
    load_ts as first_seen_ts,
    load_ts as last_seen_ts,
    load_ts as last_load_ts
from dedup
