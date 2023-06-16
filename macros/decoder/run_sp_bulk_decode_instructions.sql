{% macro run_sp_bulk_decode_instructions() %}
    {% set sql %}
    call silver.sp_bulk_decode_instructions();
{% endset %}
    {% do run_query(sql) %}
{% endmacro %}
