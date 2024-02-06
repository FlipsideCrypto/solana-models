{% macro get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) -%}
    {% set predicate_override = "" %}
    {% if incremental_predicates[0] == "dynamic_range_predicate" %}
        -- run some queries to dynamically determine the min + max of this 'date_column' in the new data
        {% set predicate_override = dynamic_range_predicate(source, incremental_predicates[1], "DBT_INTERNAL_DEST") %}
    {% endif %}
    {% set predicates = [predicate_override] if predicate_override else incremental_predicates %}
    -- standard merge from here
    {% set merge_sql = dbt.get_merge_sql(target, source, unique_key, dest_columns, predicates) %}
    {{ return(merge_sql) }}

{% endmacro %}