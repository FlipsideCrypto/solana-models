{% macro sp_bulk_decode_instructions() %}
  {% if var("UPDATE_UDFS_AND_SPS") %}
    {% set sql %}
    CREATE
    OR REPLACE PROCEDURE silver.sp_bulk_decode_instructions() returns TABLE (
      pid STRING
    ) LANGUAGE SQL EXECUTE AS owner AS
  DECLARE
    cur CURSOR FOR
  SELECT
    REPLACE(SPLIT_PART(metadata $ filename, '/', 3), '.json') program_id
  FROM
    streamline.solana.decode_instructions_idls;
  BEGIN
    FOR cur_row IN cur DO let pid VARCHAR:= cur_row.program_id;
call silver.sp_bulk_decode_instructions(:pid);
let cnt VARCHAR:= (
      SELECT
        COUNT(*)
      FROM
        TABLE(RESULT_SCAN(LAST_QUERY_ID())));
if (
          cnt > 0
        ) THEN
      INSERT INTO
        results (pid)
      VALUES
        (:pid);
    END if;
END FOR;
let rs resultset:= (
  SELECT
    *
  FROM
    results
  ORDER BY
    pid
);
-- RETURN test;
-- RETURN TABLE(RESULTSET_FROM_CURSOR(cur));
RETURN TABLE(rs);
END;
{% endset %}
{% do run_query(sql) %}
{% endif %}
{% endmacro %}
