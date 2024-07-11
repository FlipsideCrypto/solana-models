-- incremental_strategy="merge"
{% macro get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}
    {% set merge_sql = fsc_utils.get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
    {{ return(merge_sql) }}
{% endmacro %}

-- incremental_strategy="delete+insert"
{% macro get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}
    {% set predicate_override = "" %}
    -- get the min value of column
    {% if incremental_predicates[0] == "min_value_predicate" %}
        {% set min_column_name = incremental_predicates[1] %}
        {% set query %}
            select min({{ min_column_name }}) from {{ source }}
        {% endset %}
        {% set min_block = run_query(query).columns[0][0] %}

        {% if min_block is not none %}
            {% set predicate_override %}
                round({{ target }}.{{ min_column_name }},-5) >= round({{ min_block }},-5)
            {% endset %}
        {% else %}
            {% set predicate_override = "1=1" %}
        {% endif %}
    {% endif %}
    {% set predicates = [predicate_override] + incremental_predicates[2:] if predicate_override else incremental_predicates %}
    -- standard delete+insert from here
    {% set merge_sql = dbt.get_delete_insert_merge_sql(target, source, unique_key, dest_columns, predicates) %}
    {{ return(merge_sql) }}
{% endmacro %}