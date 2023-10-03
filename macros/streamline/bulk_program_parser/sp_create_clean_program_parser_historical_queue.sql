{% macro sp_create_clean_program_parser_historical_queue() %}
  {% if var("UPDATE_UDFS_AND_SPS") %}
    {% set sql %}
    CREATE OR REPLACE PROCEDURE streamline.sp_clean_program_parser_historical_queue() 
    RETURNS BOOLEAN 
    LANGUAGE SQL 
    AS 
    $$
      DECLARE
        RESULT VARCHAR;
        query_id VARCHAR;
      BEGIN
        /* review progress of all items that have been worked on before the most recent hour of activity */
        SELECT
          INDEX,
          program_id,
          instruction,
          tx_id,
          h.block_id,
          c.id as id
        from streamline.all_undecoded_instructions_history_in_progress h
        left outer join streamline.complete_decoded_instructions c
          on c.block_id = h.block_id
          and c.id = concat_ws(
              '-',
              h.block_id,
              tx_id,
              program_id,
              index
          )
        where h._inserted_timestamp <= current_timestamp - INTERVAL '1 HOURS';

        query_id := SQLID;

        /* insert items not completed back into queue */
        INSERT INTO streamline.all_undecoded_instructions_history_queue (index, program_id, instruction, tx_id, block_id)
        SELECT
          INDEX,
          program_id,
          instruction,
          tx_id,
          block_id
        FROM TABLE(RESULT_SCAN(:query_id))
        WHERE id is null;

        /* remove all in_progress items that have been reviewed */
        DELETE FROM streamline.all_undecoded_instructions_history_in_progress s
          USING (SELECT * FROM TABLE(RESULT_SCAN(:query_id))) d
          WHERE s.block_id = d.block_id
          AND s.tx_id = d.tx_id
          AND s.index = d.index
          AND s.program_id = d.program_id;        

        RETURN TRUE;
      END;
    $${% endset %}
    {% do run_query(sql) %}
  {% endif %}
{% endmacro %}