{% macro run_sp_bulk_get_stake_account_tx_ids() %}
{% set sql %}
call silver.sp_bulk_get_stake_account_tx_ids();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}