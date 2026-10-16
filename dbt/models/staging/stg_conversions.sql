{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='conversion_id',
    partition_by={'field': 'conversion_date', 'data_type': 'date'},
    cluster_by=['session_id']
) }}

with src as (
    select * from {{ source('bronze', 'raw_conversions') }}
    {% if is_incremental() %}
    where _ingested_at > {{ incremental_watermark() }}
    {% endif %}
),

parsed as (
    select
        conversion_id,
        session_id,
        lead_status,
        buyer_id,
        revenue_usd,
        partner_payout_usd,
        safe.parse_timestamp('%Y-%m-%dT%H:%M:%E*SZ', `timestamp`) as conversion_ts,
        _ingested_at,
        _batch_id
    from src
)

select
    conversion_id,
    session_id,
    lead_status,
    buyer_id,
    revenue_usd,
    partner_payout_usd,
    conversion_ts,
    date(conversion_ts) as conversion_date,
    -- Late-arriving conversions can carry a timestamp after the moment we ingested them; keep and flag them.
    conversion_ts > _ingested_at as is_late_arriving,
    _ingested_at,
    _batch_id
from parsed
qualify row_number() over (partition by conversion_id order by _ingested_at desc, _batch_id desc) = 1
