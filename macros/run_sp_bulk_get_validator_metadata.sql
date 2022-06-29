{% macro run_sp_bulk_get_validator_metadata() %}
{% set sql %}
call silver.sp_bulk_get_validator_metadata();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}