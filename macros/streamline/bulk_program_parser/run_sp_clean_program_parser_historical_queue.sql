{% macro run_sp_clean_program_parser_historical_queue() %}
{% set sql %}
call streamline.sp_clean_program_parser_historical_queue();
{% endset %}

{% do run_query(sql) %}
{% endmacro %}