{#- Use the configured dataset name as-is (silver / gold) instead of dbt's default <target>_<custom> prefix. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
