{% macro sp_create_snapshot_get_vote_accounts() %}
  {% if var("UPDATE_UDFS_AND_SPS") %}
    {% set sql %}
    CREATE OR REPLACE PROCEDURE silver.sp_snapshot_get_vote_accounts() 
    RETURNS variant 
    LANGUAGE SQL 
    AS 
    $$
      DECLARE
        RESULT VARCHAR;
      BEGIN
        RESULT:= (
            SELECT
              silver.udf_snapshot_get_vote_accounts()
          );
        RETURN RESULT;
      END;
    $${% endset %}
    {% do run_query(sql) %}
  {% endif %}
{% endmacro %}