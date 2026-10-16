{#-
    High-water mark for incremental models, rendered as a constant timestamp literal.
    BigQuery can only skip (prune) partitions when the filter is a constant; a subquery
    such as (select max(...) from this) forces a scan of every bronze partition.
-#}
{% macro incremental_watermark(column='_ingested_at') -%}
    {#- Unit tests mock `this`, so keep the subquery there; run_query would hit the live table instead. -#}
    {%- if model.resource_type == 'unit_test' or not execute -%}
        (select coalesce(max({{ column }}), timestamp('1970-01-01')) from {{ this }})
    {%- else -%}
        {%- set sql -%}
            select format_timestamp('%F %H:%M:%E6S', coalesce(max({{ column }}), timestamp('1970-01-01')), 'UTC') from {{ this }}
        {%- endset -%}
        timestamp('{{ run_query(sql).columns[0].values()[0] }}')
    {%- endif -%}
{%- endmacro %}
