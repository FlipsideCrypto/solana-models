{% macro sp_refresh_external_tables_full() %}
{% set sql %}
create or replace procedure streamline.sp_refresh_external_tables_full()
returns boolean
language sql
execute as caller
as
$$
    begin 
        alter external table streamline.{{ target.database }}.decoded_instructions_data_api refresh;
        alter external table streamline.{{ target.database }}.validator_metadata_api refresh;
        return TRUE;
    end;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}