{# Use +schema names as-is (staging, marts) instead of dbt's default "<target>_<name>". #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
