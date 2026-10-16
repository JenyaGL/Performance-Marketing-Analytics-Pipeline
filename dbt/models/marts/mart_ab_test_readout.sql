-- Frequentist two-proportion z-test, control vs treatment, one row per funnel metric.
-- n = sessions with a landing_view; x = those sessions that reached the metric's step.

{% set control = var('ab_control_variant') %}
{% set treatment = var('ab_treatment_variant') %}

with sessions as (
    select
        session_id,
        any_value(variant_id) as variant_id,
        logical_or(step_name = 'landing_view') as reached_landing,
        logical_or(step_name = 'form_start') as reached_form_start,
        logical_or(step_name = 'form_submit') as reached_submit
    from {{ ref('stg_funnel_events') }}
    group by session_id
),

per_variant as (
    select
        variant_id,
        countif(reached_landing) as n,
        countif(reached_landing and reached_form_start) as x_form_start,
        countif(reached_landing and reached_submit) as x_submit
    from sessions
    group by variant_id
),

metrics as (
    select 'landing_to_form_start' as metric_name, variant_id, n, x_form_start as x from per_variant
    union all
    select 'landing_to_submit' as metric_name, variant_id, n, x_submit as x from per_variant
),

pivoted as (
    select
        metric_name,
        sum(if(variant_id = '{{ control }}', n, 0)) as n_c,
        sum(if(variant_id = '{{ control }}', x, 0)) as x_c,
        sum(if(variant_id = '{{ treatment }}', n, 0)) as n_t,
        sum(if(variant_id = '{{ treatment }}', x, 0)) as x_t
    from metrics
    group by metric_name
),

rates as (
    select
        *,
        x_c / nullif(n_c, 0) as p_c,
        x_t / nullif(n_t, 0) as p_t,
        (x_c + x_t) / nullif(n_c + n_t, 0) as pooled_p
    from pivoted
),

stats as (
    select
        *,
        p_t - p_c as abs_lift,
        sqrt(pooled_p * (1 - pooled_p) * (1 / nullif(n_c, 0) + 1 / nullif(n_t, 0))) as std_error
    from rates
)

select
    metric_name,
    '{{ control }}' as control_variant,
    '{{ treatment }}' as treatment_variant,
    n_c,
    x_c,
    round(p_c, 6) as p_c,
    n_t,
    x_t,
    round(p_t, 6) as p_t,
    round(abs_lift, 6) as abs_lift,
    round(abs_lift / nullif(p_c, 0), 6) as rel_lift,
    round(pooled_p, 6) as pooled_p,
    round(std_error, 6) as std_error,
    round(abs_lift / nullif(std_error, 0), 4) as z_score,
    -- No variance (SE = 0) or missing data => z is NULL => not significant.
    coalesce(abs(abs_lift / nullif(std_error, 0)) >= {{ var('ab_z_critical') }}, false) as is_statistically_significant
from stats
