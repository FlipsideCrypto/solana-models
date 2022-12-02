{% macro task_bulk_get_blocks_real_time() %}
{% set sql %}
execute immediate 'create or replace task streamline.bulk_get_blocks_real_time
    warehouse = dbt_cloud
    allow_overlapping_execution = false
    schedule = \'USING CRON */15 * * * * UTC\'
as
BEGIN
    create or replace temporary table streamline.complete_blocks__dbt_tmp as
    (
        select * 
        from (
                WITH meta AS (
                    SELECT
                        registered_on,
                        file_name
                    FROM
                        TABLE(
                            information_schema.external_table_files(
                                table_name => \'streamline.{{ target.database }}.blocks_api\'
                            )
                        ) A
                    WHERE
                        registered_on >= (
                            SELECT
                                COALESCE(MAX(_INSERTED_TIMESTAMP), \'1970-01-01\' :: DATE) max_INSERTED_TIMESTAMP
                            FROM
                                streamline.complete_blocks
                        )
                )
                SELECT
                    block_id,
                    error,
                    _inserted_date,
                    m.registered_on as _inserted_timestamp
                FROM
                    streamline.{{ target.database }}.blocks_api AS s
                    JOIN meta m
                    ON m.file_name = metadata$filename
                WHERE
                    s.block_id IS NOT NULL
                    AND s._inserted_date >= CURRENT_DATE
                    AND m.registered_on > (
                        SELECT
                            coalesce(max(_inserted_timestamp),\'2022-01-01 00:00:00\'::timestamp_ntz)
                        FROM
                            streamline.complete_blocks
                    )
                    qualify(ROW_NUMBER() over (PARTITION BY block_id
                    ORDER BY
                    _inserted_date, _inserted_timestamp DESC)) = 1
        ) 
        order by (_inserted_date)
    );
    merge into streamline.complete_blocks as DBT_INTERNAL_DEST
        using streamline.complete_blocks__dbt_tmp as DBT_INTERNAL_SOURCE
        on DBT_INTERNAL_SOURCE.block_id = DBT_INTERNAL_DEST.block_id
        when matched then 
            update set
            _inserted_date = DBT_INTERNAL_SOURCE._inserted_date,
            _inserted_timestamp = DBT_INTERNAL_SOURCE._inserted_timestamp,
            error = DBT_INTERNAL_SOURCE.error
        when not matched then 
            insert ("BLOCK_ID", "ERROR", "_INSERTED_DATE", "_INSERTED_TIMESTAMP")
            values ("BLOCK_ID", "ERROR", "_INSERTED_DATE", "_INSERTED_TIMESTAMP");
    select streamline.udf_bulk_get_blocks(TRUE)
    where exists (
        select 1
        from streamline.all_unknown_blocks_real_time
        limit 1
    );
END;'
{% endset %}
{% do run_query(sql) %}

{% if target.database == 'SOLANA' %}
    {% set sql %}
    alter task streamline.bulk_get_blocks_real_time resume;
    {% endset %}
    {% do run_query(sql) %}
{% endif %}
{% endmacro %}