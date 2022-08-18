{% macro task_bulk_get_block_rewards_historical() %}
create or replace task streamline.bulk_get_block_rewards_historical
    warehouse = dbt_cloud_solana
    allow_overlapping_execution = false
    schedule = 'USING CRON */3 * * * * UTC'
as
BEGIN
    call streamline.refresh_external_table_next_batch('block_rewards_api','complete_block_rewards');
    create or replace temporary table streamline.complete_block_rewards__dbt_tmp as
    (
        select * 
        from (
            SELECT
                block_id,
                _partition_id
            FROM
                bronze.block_rewards_api AS s
            WHERE
                s.block_id IS NOT NULL
                AND s._partition_id > (
                    select 
                        coalesce(max(_partition_id),0)
                    from
                        streamline.complete_block_rewards
                )

            group by 1,2
        ) 
        order by (_partition_id)
    );
    merge into streamline.complete_block_rewards as DBT_INTERNAL_DEST
        using streamline.complete_block_rewards__dbt_tmp as DBT_INTERNAL_SOURCE
        on DBT_INTERNAL_SOURCE.block_id = DBT_INTERNAL_DEST.block_id
        when matched then 
            update set
            _partition_id = DBT_INTERNAL_SOURCE._partition_id
        when not matched then 
            insert ("BLOCK_ID", "_PARTITION_ID")
            values ("BLOCK_ID", "_PARTITION_ID");
    select streamline.udf_bulk_get_block_rewards()
        where exists (
            select 1
            from streamline.all_unknown_block_rewards_historical
            limit 1
        );       
END;

alter task streamline.bulk_get_block_rewards_historical resume
{% endmacro %}