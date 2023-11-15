{{ config(
    materialized = 'incremental',
    unique_key = "collection_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}

SELECT
    collection_id,
    DATA :data :data [0] :nft_collection_name :: STRING AS nft_collection_name,
    {{ dbt_utils.generate_surrogate_key(['nft_collection_name']) }} AS solscan_nft_collections_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__solscan_nft_collections') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    AND collection_id NOT IN (
        SELECT
            collection_id
        FROM
            {{ this }}
    )
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY collection_id
ORDER BY
    _inserted_timestamp DESC)) = 1
