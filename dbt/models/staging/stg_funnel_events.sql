{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_id',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['session_id']
) }}

with src as (
    select * from {{ source('bronze', 'raw_events') }}
    {% if is_incremental() %}
    where _ingested_at > {{ incremental_watermark() }}
    {% endif %}
),

parsed as (
    select
        event_id,
        session_id,
        traffic_id,
        user_pseudo_id,
        step_name,
        step_number,
        variant_id,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', `timestamp`) as event_ts,
        _ingested_at,
        _batch_id
    from src
)

select
    event_id,
    session_id,
    traffic_id,
    user_pseudo_id,
    step_name,
    step_number,
    variant_id,
    event_ts,
    date(event_ts) as event_date,
    _ingested_at,
    _batch_id
from parsed
qualify row_number() over (partition by event_id order by _ingested_at desc, _batch_id desc) = 1
