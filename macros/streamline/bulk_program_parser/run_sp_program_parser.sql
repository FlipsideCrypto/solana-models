{% macro run_sp_udf_bulk_program_parser() %}
{% set sql %}
call silver.sp_udf_bulk_program_parser();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}