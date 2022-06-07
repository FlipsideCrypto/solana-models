{% macro create_udfs() %}
    {% set sql %}
    {{ udf_bulk_get_decoded_instructions_data() }};
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
