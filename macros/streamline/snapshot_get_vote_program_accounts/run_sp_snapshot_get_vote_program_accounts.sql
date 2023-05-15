{% macro run_sp_snapshot_get_vote_program_accounts() %}
{% set sql %}
call silver.sp_snapshot_get_vote_program_accounts();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}