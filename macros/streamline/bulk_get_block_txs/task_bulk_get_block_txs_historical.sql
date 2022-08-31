{% macro task_bulk_get_block_txs_historical() %}
{% set sql %}
execute immediate 'create or replace task streamline.bulk_get_block_txs_historical
    warehouse = dbt_cloud_solana
    allow_overlapping_execution = false
    schedule = \'USING CRON */15 * * * * UTC\'
as
BEGIN
    select streamline.udf_bulk_get_block_txs(FALSE)
    where exists (
        select 1
        from streamline.all_unknown_block_txs_historical
        limit 1
    );
END;'
{% endset %}
{% do run_query(sql) %}

{% if target.database == 'SOLANA' %}
    {% set sql %}
    alter task streamline.bulk_get_block_txs_historical resume;
    {% endset %}
    {% do run_query(sql) %}
{% endif %}
{% endmacro %}