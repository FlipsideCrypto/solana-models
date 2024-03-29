{% macro task_bulk_get_block_txs_historical() %}
{% set sql %}
execute immediate 'create or replace task streamline.bulk_get_block_txs_historical
    warehouse = dbt_cloud
    allow_overlapping_execution = false
    schedule = \'USING CRON */20 * * * * UTC\'
as
BEGIN
    create or replace temporary table streamline.complete_block_txs__dbt_tmp as
    (
        select * 
        from (
            SELECT
                block_id,
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
            group by 1,2
        ) 
        order by (_partition_id)
    );
    merge into streamline.complete_block_txs as DBT_INTERNAL_DEST
        using streamline.complete_block_txs__dbt_tmp as DBT_INTERNAL_SOURCE
        on DBT_INTERNAL_SOURCE.block_id = DBT_INTERNAL_DEST.block_id
        when matched then 
            update set _partition_id = DBT_INTERNAL_SOURCE._partition_id
        when not matched then 
            insert ("BLOCK_ID", "_PARTITION_ID")
            values ("BLOCK_ID", "_PARTITION_ID");
    select streamline.udf_bulk_get_block_txs(FALSE)
    where exists (
        select 1
        from streamline.all_unknown_block_txs_historical
        limit 1
    );
    select streamline.udf_bulk_get_block_txs(TRUE)
    where exists (
        select 1
        from streamline.all_unknown_block_txs_real_time
        limit 1
    );
END;'
{% endset %}
{% do run_query(sql) %}

{% set sql %}
    alter task streamline.bulk_get_block_txs_historical suspend;
{% endset %}
{% do run_query(sql) %}
{% endmacro %}