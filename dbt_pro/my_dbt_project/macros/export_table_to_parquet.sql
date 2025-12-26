{% macro export_tables_to_parquet(schema_name, tables, output_path) %}

{% for table in tables %}
    COPY {{ schema_name }}.{{ table }}
    TO '{{ output_path }}/{{ table }}'
    (FORMAT PARQUET, OVERWRITE_OR_IGNORE TRUE);
{% endfor %}

{% endmacro %}
