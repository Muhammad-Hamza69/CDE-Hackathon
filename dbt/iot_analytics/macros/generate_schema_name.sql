{#
  Task 2.2 fixes exact schema names (RAW/CLEAN/ANALYTICS = Bronze/Silver/Gold).
  dbt's default generate_schema_name concatenates "<target_schema>_<custom_schema>",
  which would produce e.g. "HACKATHON_IOT_analytics" instead of "ANALYTICS".
  This override uses the custom schema verbatim so `+schema:` in
  dbt_project.yml maps 1:1 onto the required schema names.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
