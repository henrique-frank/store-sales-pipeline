{{ config(
    materialized='incremental',
    unique_key='dim_store_key',
    incremental_strategy='merge'
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
),

latest as (
    select *
    from src
    qualify row_number() over (
        partition by store_token
        order by load_ts desc
    ) = 1
)

{% if is_incremental() %}

, existing_current as (
    select * from {{ this }} where is_current = true
)

, changes as (
    select
        l.store_token,
        l.store_group,
        l.store_name,
        l.load_ts,
        e.dim_store_key         as old_key,
        e.valid_from            as old_valid_from,
        e.first_seen_ts         as old_first_seen,
        e.store_group           as old_group,
        e.store_name            as old_name,
        case
            when e.store_token is null then 'NEW'
            when e.store_group != l.store_group
              or e.store_name  != l.store_name then 'CHANGED'
            else 'SAME'
        end as change_status
    from latest l
    left join existing_current e on e.store_token = l.store_token
)

-- Close old rows: merge matches on old dim_store_key → UPDATE
select
    old_key                             as dim_store_key,
    store_token,
    old_group                           as store_group,
    old_name                            as store_name,
    old_valid_from                      as valid_from,
    load_ts                             as valid_to,
    false                               as is_current,
    old_first_seen                      as first_seen_ts,
    load_ts                             as last_load_ts
from changes
where change_status = 'CHANGED'

union all

-- Insert new/changed rows: new dim_store_key → INSERT
select
    md5(store_token || '|' || load_ts::string)  as dim_store_key,
    store_token,
    store_group,
    store_name,
    load_ts                                      as valid_from,
    null::timestamp_ntz                          as valid_to,
    true                                         as is_current,
    coalesce(old_first_seen, load_ts)            as first_seen_ts,
    load_ts                                      as last_load_ts
from changes
where change_status in ('NEW', 'CHANGED')

{% else %}

-- Initial full load
select
    md5(store_token || '|' || load_ts::string)  as dim_store_key,
    store_token,
    store_group,
    store_name,
    load_ts                                      as valid_from,
    null::timestamp_ntz                          as valid_to,
    true                                         as is_current,
    load_ts                                      as first_seen_ts,
    load_ts                                      as last_load_ts
from latest

{% endif %}
