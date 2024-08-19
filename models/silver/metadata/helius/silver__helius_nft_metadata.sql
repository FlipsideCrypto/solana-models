{{ config(
    materialized = 'incremental',
    unique_key = "mint",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(mint,nft_name,nft_collection_id)'),
    tags = ['nft_api']
) }}

with pre_final as (
SELECT
    items.value ['id'] :: STRING AS mint,
    items.value ['authorities'] AS authorities,
    items.value ['content'] AS content,
    items.value ['content'] ['metadata'] AS metadata,
    items.value ['creators'] AS creators,
    items.value ['grouping'][0]:group_value::string AS group_value,
    items.value ['supply'] AS supply,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__helius_nft_metadata') }},
    LATERAL FLATTEN(
        input => DATA :data :result
    ) AS items
    WHERE mint is not NULL
{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
    qualify(ROW_NUMBER() over (PARTITION BY mint
ORDER BY
    _inserted_timestamp DESC)) = 1)

select 
    mint,
    authorities[0]:address::string as authority,
    creators,
    metadata:attributes::array as metadata,
    content:links:image::string as image_url,
    content:json_uri::string as metadata_uri,
    metadata:name::string as nft_name,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['mint']) }} AS helius_nft_metadata_id,
    CASE 
        WHEN group_value IS NULL 
        THEN NULL 
        ELSE {{ dbt_utils.generate_surrogate_key(['group_value']) }} 
    END AS nft_collection_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
from pre_final


