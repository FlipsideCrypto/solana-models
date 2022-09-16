{% macro sp_refresh_external_tables_full() %}
{% set sql %}
create or replace procedure streamline.sp_refresh_external_tables_full()
returns boolean
language sql
as
$$
    begin 
        alter external table streamline.{{ target.database }}.decoded_instructions_data_api refresh;
        alter external table streamline.{{ target.database }}.validator_metadata_api refresh;
        alter external table streamline.{{ target.database }}.stake_account_tx_ids_api refresh;
        alter external table streamline.{{ target.database }}.txs_api refresh;
        return TRUE;
    end;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}