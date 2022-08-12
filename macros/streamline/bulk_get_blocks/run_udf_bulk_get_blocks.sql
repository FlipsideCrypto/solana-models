{% macro run_udf_bulk_get_blocks() %}
{% set sql %}
SELECT
    streamline.udf_bulk_get_blocks()
WHERE
    EXISTS(
        SELECT
            1
        FROM
            streamline.all_unknown_blocks_historical
        LIMIT 1
    )
{% endset %}

{% do run_query(sql) %}
{% endmacro %}