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
        alter external table streamline.{{ target.database }}.validator_vote_accounts refresh;
        alter external table streamline.{{ target.database }}.validators_app_list_api refresh;
        alter external table streamline.{{ target.database }}.stake_program_accounts refresh;
        alter external table streamline.{{ target.database }}.validator_vote_program_accounts refresh;
        return TRUE;
    end;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}