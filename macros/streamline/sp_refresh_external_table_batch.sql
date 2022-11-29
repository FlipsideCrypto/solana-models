{% macro sp_refresh_external_table_batch() %}
{% set sql %}
create or replace procedure streamline.refresh_external_table_next_batch(external_table_name string, streamline_table_name string)
returns string
language sql
as
$$
    declare 
        path string;
        select_stmt string;
        refresh_stmt string;
        res resultset;
    begin 
        select_stmt := 'select concat(\'batch=\',coalesce(max(_partition_id),0)+1,\'/\') as path from streamline.' || :streamline_table_name;
        res := (execute immediate :select_stmt);
        let c1 cursor for res;
        for row_variable in c1 do
            path := row_variable.path;
        end for;
        refresh_stmt := 'alter external table streamline.{{ target.database }}.' || :external_table_name || ' refresh \'' || :PATH || '\'';
        res := (execute immediate :refresh_stmt);
        return 'table refreshed with ' || :refresh_stmt;
    end;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}