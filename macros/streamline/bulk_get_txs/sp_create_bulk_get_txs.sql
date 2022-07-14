{% macro sp_create_bulk_get_txs() %}
{% set sql %}
CREATE OR REPLACE PROCEDURE silver.sp_bulk_get_txs() 
RETURNS variant 
LANGUAGE SQL 
AS 
$$
  DECLARE
    RESULT VARCHAR;
    row_cnt INTEGER;
  BEGIN
    row_cnt:= (
      SELECT
        COUNT(1)
      FROM
        silver._all_unknown_tx_details
    );
    if (
        row_cnt > 0
      ) THEN RESULT:= (
        SELECT
          silver.udf_bulk_get_txs()
      );
      ELSE RESULT:= NULL;
    END if;
    RETURN RESULT;
  END;
$${% endset %}
{% do run_query(sql) %}
{% endmacro %}