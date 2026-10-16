{{ config(
    partition_by={'field': 'report_date', 'data_type': 'date'},
    cluster_by=['partner_id', 'campaign_id']
) }}

-- Grain: click date x partner x campaign x traffic source.
-- Sessions and conversions are attributed back to the click (traffic_id), so a late-arriving
-- conversion lands on its original click date when this table is rebuilt.

with traffic as (
    select
        traffic_id,
        traffic_date as report_date,
        partner_id,
        campaign_id,
        traffic_source,
        clicks,
        cost_usd
    from {{ ref('stg_ad_traffic') }}
),

-- Roll events up to one row per session before joining, so the join cannot fan out.
sessions as (
    select
        traffic_id,
        session_id,
        logical_or(step_name = 'form_start') as reached_form_start
    from {{ ref('stg_funnel_events') }}
    group by traffic_id, session_id
),

conversions as (
    select
        session_id,
        count(*) as leads,
        countif(lead_status = 'accepted') as accepted_leads,
        sum(revenue_usd) as revenue,
        sum(partner_payout_usd) as partner_payout
    from {{ ref('stg_conversions') }}
    group by session_id
),

funnel_per_traffic as (
    select
        s.traffic_id,
        count(*) as sessions,
        countif(s.reached_form_start) as form_starts,
        sum(coalesce(c.leads, 0)) as leads,
        sum(coalesce(c.accepted_leads, 0)) as accepted_leads,
        sum(coalesce(c.revenue, 0)) as revenue,
        sum(coalesce(c.partner_payout, 0)) as partner_payout
    from sessions as s
    left join conversions as c using (session_id)
    group by s.traffic_id
),

daily as (
    select
        t.report_date,
        t.partner_id,
        t.campaign_id,
        t.traffic_source,
        sum(t.cost_usd) as spend,
        sum(t.clicks) as clicks,
        sum(coalesce(f.sessions, 0)) as sessions,
        sum(coalesce(f.form_starts, 0)) as form_starts,
        sum(coalesce(f.leads, 0)) as leads,
        sum(coalesce(f.accepted_leads, 0)) as accepted_leads,
        sum(coalesce(f.revenue, 0)) as revenue,
        sum(coalesce(f.partner_payout, 0)) as partner_payout
    from traffic as t
    left join funnel_per_traffic as f using (traffic_id)
    group by 1, 2, 3, 4
)

select
    report_date,
    partner_id,
    campaign_id,
    traffic_source,
    round(spend, 2) as spend,
    clicks,
    sessions,
    form_starts,
    leads,
    accepted_leads,
    round(revenue, 2) as revenue,
    round(partner_payout, 2) as partner_payout,
    round(revenue - spend, 2) as margin,
    -- Defensive ratios: NULL (not an error, not 0) when the denominator is 0.
    round(spend / nullif(leads, 0), 4) as cpl,
    round(revenue / nullif(clicks, 0), 4) as epc,
    round(leads / nullif(clicks, 0), 4) as ctr_to_lead
from daily
