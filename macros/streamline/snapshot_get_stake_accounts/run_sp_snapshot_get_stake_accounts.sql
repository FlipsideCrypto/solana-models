{% macro run_sp_snapshot_get_stake_accounts() %}
{% set sql %}
call silver.sp_snapshot_get_stake_accounts();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}