{% macro get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}
    {% set merge_sql = fsc_utils.get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
    {{ return(merge_sql) }}
{% endmacro %}