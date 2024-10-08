{{ config (
    materialized = "incremental",
    unique_key = "block_id",
    cluster_by = "_partition_id",
    merge_update_columns = ["_partition_id"],
    tags = ['streamline'],
    full_refresh = false,
    enabled = false,
) }}

SELECT
    block_id,
    error,
    _partition_id
FROM
    {{ source(
        "bronze_streamline",
        "block_rewards_api"
    ) }} AS s
WHERE
    s.block_id IS NOT NULL

{% if is_incremental() %}
AND s._partition_id > (
    select 
        coalesce(max(_partition_id),0)
    from
        {{ this }}
)
{% endif %}
group by 1,2,3
