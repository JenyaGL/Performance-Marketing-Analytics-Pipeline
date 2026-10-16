{{ config(partition_by={'field': 'report_date', 'data_type': 'date'}) }}

-- One row per day of KPI totals, ready to filter by date.
-- Rates are recomputed from the summed totals; averaging the detailed mart's rates would be wrong.

with totals as (
    select
        report_date,
        sum(spend) as spend,
        sum(clicks) as clicks,
        sum(sessions) as sessions,
        sum(form_starts) as form_starts,
        sum(leads) as leads,
        sum(accepted_leads) as accepted_leads,
        sum(revenue) as revenue,
        sum(partner_payout) as partner_payout
    from {{ ref('mart_performance_funnel_daily') }}
    group by report_date
)

select
    report_date,
    round(spend, 2) as spend,
    clicks,
    sessions,
    form_starts,
    leads,
    accepted_leads,
    round(revenue, 2) as revenue,
    round(partner_payout, 2) as partner_payout,
    round(revenue - spend, 2) as margin,
    round(spend / nullif(leads, 0), 4) as cpl,
    round(revenue / nullif(clicks, 0), 4) as epc,
    round(leads / nullif(clicks, 0), 4) as lead_rate,
    round(accepted_leads / nullif(leads, 0), 4) as acceptance_rate
from totals
