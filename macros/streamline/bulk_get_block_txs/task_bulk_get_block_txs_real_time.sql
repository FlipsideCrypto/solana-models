{% macro task_bulk_get_block_txs_real_time() %}
{% set sql %}
execute immediate 'create or replace task streamline.bulk_get_block_txs_real_time
    warehouse = dbt_cloud
    allow_overlapping_execution = false
    schedule = \'USING CRON */20 * * * * UTC\'
as
BEGIN
    call streamline.refresh_external_table_next_batch(\'block_txs_api\',\'complete_block_txs\');
    create or replace temporary table streamline.complete_block_txs__dbt_tmp as
    (
        select * 
        from (
            SELECT
                block_id,
                error,
                _partition_id
            FROM
                streamline.{{ target.database }}.block_txs_api AS s
            WHERE
                s.block_id IS NOT NULL
            AND s._partition_id > (
                select 
                    coalesce(max(_partition_id),0)
                from
                    streamline.complete_block_txs
            )
            group by 1,2,3
        ) 
        order by (_partition_id)
    );
    merge into streamline.complete_block_txs as DBT_INTERNAL_DEST
        using streamline.complete_block_txs__dbt_tmp as DBT_INTERNAL_SOURCE
        on DBT_INTERNAL_SOURCE.block_id = DBT_INTERNAL_DEST.block_id
        when matched then 
            update 
                set _partition_id = DBT_INTERNAL_SOURCE._partition_id,
                    error = DBT_INTERNAL_SOURCE.error
        when not matched then 
            insert ("BLOCK_ID", "ERROR", "_PARTITION_ID")
            values ("BLOCK_ID", "ERROR", "_PARTITION_ID");
    select streamline.udf_bulk_get_block_txs(TRUE)
    where exists (
        select 1
        from streamline.all_unknown_block_txs_real_time
        limit 1
    );
END;'
{% endset %}
{% do run_query(sql) %}

{% if target.database == 'SOLANA' %}
    {% set sql %}
    alter task streamline.bulk_get_block_txs_real_time resume;
    {% endset %}
    {% do run_query(sql) %}
{% endif %}
{% endmacro %}