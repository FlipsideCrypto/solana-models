{% macro run_sp_bulk_get_txs() %}
{% set sql %}
call silver.sp_bulk_get_txs();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}