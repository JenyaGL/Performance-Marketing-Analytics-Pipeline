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
),

deduped as (
    select *
    from parsed
    qualify row_number() over (partition by conversion_id order by _ingested_at desc, _batch_id desc) = 1
),

-- One row per session, so the join below cannot fan out.
submits as (
    select session_id, min(event_ts) as submitted_at
    from {{ ref('stg_funnel_events') }}
    where step_name = 'form_submit'
    group by session_id
),

with_lag as (
    select
        d.*,
        timestamp_diff(d.conversion_ts, s.submitted_at, second) as conversion_lag_seconds
    from deduped as d
    left join submits as s using (session_id)
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
    -- Seconds from the session's form_submit to the conversion; NULL when no submit event was found.
    conversion_lag_seconds,
    -- Late = the buyer's conversion record lagged the form submit by more than the threshold.
    coalesce(conversion_lag_seconds > {{ var('late_arrival_threshold_minutes') }} * 60, false) as is_late_arriving,
    _ingested_at,
    _batch_id
from with_lag
