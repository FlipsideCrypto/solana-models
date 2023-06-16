{% macro sp_bulk_decode_instructions() %}
  {% if var("UPDATE_UDFS_AND_SPS") %}
    {% set sql %}
CREATE OR REPLACE PROCEDURE silver.sp_bulk_decode_instructions()
RETURNS TABLE (pid string)
LANGUAGE SQL
EXECUTE AS OWNER
AS DECLARE
  cur CURSOR FOR  
  Select REPLACE(SPLIT_PART(metadata$filename, '/', 3), '.json') program_id from streamline.solana.decode_instructions_idls    ;
BEGIN
  
    FOR cur_row IN cur DO
        let pid varchar:= cur_row.program_id;
        call silver.sp_bulk_decode_instructions(:pid);
        let cnt varchar := (select count(*) from table(result_scan(last_query_id())));
        if (cnt > 0) then
            insert into results (pid) values (:pid);
        end if;
    END FOR;
    let rs resultset := (select * from results order by pid);
    -- RETURN test;
    -- RETURN TABLE(RESULTSET_FROM_CURSOR(cur));
    RETURN TABLE(rs);
END;{% endset %}
{% do run_query(sql) %}
{% endif %}
{% endmacro %}
