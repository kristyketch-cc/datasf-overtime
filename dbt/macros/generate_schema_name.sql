{#
  By default dbt builds a schema name by joining the target schema and the
  model's schema, giving names like STAGING_STAGING. This project separates
  dev and prod by database instead, so a model configured with
  "+schema: staging" should land in a schema called exactly STAGING.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
