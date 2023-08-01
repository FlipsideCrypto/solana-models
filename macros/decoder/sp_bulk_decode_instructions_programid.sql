{% macro sp_bulk_decode_instructions_programid() %}
  {% if var("UPDATE_UDFS_AND_SPS") %}
    {% set sql %}
    CREATE OR REPLACE PROCEDURE silver.sp_bulk_decode_instructions(program_id VARCHAR(16777216))
RETURNS table()
LANGUAGE SQL
AS 
DECLARE
  results resultset;
  exec_sql varchar default '';
BEGIN
    select concat(' ', 
        'Insert into bronze.decoded_instructions
      SELECT  tx_id, index,
         silver.udf_decode_instructions(program_id,instruction)  as decoded_instruction
         , block_timestamp
          ,program_id
        from silver._all_undecoded_instructions
        WHERE lower(program_id) = lower(''', :program_id, ''')
        Limit 1000
        ;')
     into :exec_sql;
    results := (EXECUTE IMMEDIATE :exec_sql );
  RETURN table(results);
END; {% endset %}
{% do run_query(sql) %}
{% endif %}
{% endmacro %}
