{% macro sp_create_bulk_get_decoded_instructions_data() %}
  {% if var("UPDATE_UDFS_AND_SPS") %}
    {% set sql %}
    CREATE OR REPLACE PROCEDURE silver.sp_bulk_get_decoded_instructions_data() 
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
            silver._all_undecoded_instructions_data
        );
        if (
            row_cnt > 0
          ) THEN RESULT:= (
            SELECT
              silver.udf_bulk_get_decoded_instructions_data()
          );
          ELSE RESULT:= NULL;
        END if;
        RETURN RESULT;
      END;
    $${% endset %}
    {% do run_query(sql) %}
  {% endif %}
{% endmacro %}