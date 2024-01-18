{% macro dbt_snowflake_get_tmp_relation_type(strategy, unique_key, language) %}
    -- always table
    {{ return('table') }}
{% endmacro %}