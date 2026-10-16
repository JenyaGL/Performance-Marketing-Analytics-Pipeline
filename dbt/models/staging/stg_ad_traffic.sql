{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='traffic_id',
    partition_by={'field': 'traffic_date', 'data_type': 'date'},
    cluster_by=['partner_id', 'campaign_id']
) }}

with src as (
    select * from {{ source('bronze', 'raw_traffic') }}
    {% if is_incremental() %}
    where _ingested_at > {{ incremental_watermark() }}
    {% endif %}
),

parsed as (
    select
        traffic_id,
        partner_id,
        campaign_id,
        vertical,
        traffic_source,
        clicks,
        cost_usd,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', `timestamp`) as traffic_ts,
        _ingested_at,
        _batch_id
    from src
)

select
    traffic_id,
    partner_id,
    campaign_id,
    vertical,
    traffic_source,
    clicks,
    cost_usd,
    traffic_ts,
    date(traffic_ts) as traffic_date,
    _ingested_at,
    _batch_id
from parsed
qualify row_number() over (partition by traffic_id order by _ingested_at desc, _batch_id desc) = 1
