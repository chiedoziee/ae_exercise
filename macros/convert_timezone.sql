{% macro convert_timezone(column_name) -%}

datetime({{column_name}}, "America/Montreal")

{%- endmacro %}
