{{ config (
    materialized = "incremental",
    unique_key = "block_id",
    cluster_by = "_partition_id",
    merge_update_columns = ["_partition_id"]
) }}

WITH meta AS (

    SELECT
        registered_on,
        file_name
    FROM
        TABLE(
            information_schema.external_table_files(
                table_name => '{{ source( "solana_external", "block_txs_api") }}'
            )
        ) A
)
SELECT
    block_id,
    _partition_id
FROM
    {{ source(
        "solana_external",
        "block_txs_api"
    ) }} AS s
WHERE
    s.block_id IS NOT NULL

{% if is_incremental() %}
AND s._partition_id > (
    select 
        max(_partition_id)
    from
        {{ this }}
)
{% endif %}
group by 1,2
