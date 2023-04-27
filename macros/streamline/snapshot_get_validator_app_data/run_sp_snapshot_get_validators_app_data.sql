{% macro run_sp_snapshot_get_validators_app_data() %}
{% set sql %}
call silver.sp_snapshot_get_validators_app_data();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}