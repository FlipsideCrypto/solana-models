{% macro sp_create_bulk_get_validator_metadata() %}
  {% if var("UPDATE_UDFS_AND_SPS") %}
    {% set sql %}
    CREATE OR REPLACE PROCEDURE silver.sp_bulk_get_validator_metadata() 
    RETURNS variant 
    LANGUAGE SQL 
    AS 
    $$
      DECLARE
        RESULT VARCHAR;
      BEGIN
        RESULT:= (
            SELECT
              silver.udf_bulk_get_validator_metadata()
          );
        RETURN RESULT;
      END;
    $${% endset %}
    {% do run_query(sql) %}
  {% endif %}
{% endmacro %}