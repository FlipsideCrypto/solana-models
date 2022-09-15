{% macro run_sp_refresh_external_tables_full() %}
{% set sql %}
call streamline.sp_refresh_external_tables_full();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}