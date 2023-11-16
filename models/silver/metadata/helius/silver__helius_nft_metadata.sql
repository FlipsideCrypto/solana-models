{{ config(
    materialized = 'incremental',
    unique_key = "mint",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['nft_api']
) }}

with pre_final as (
SELECT
    items.value ['id'] :: STRING AS mint,
    items.value ['authorities'] AS authorities,
    -- items.value ['burnt']::boolean AS burnt,
    -- items.value ['compression'] AS compression,
    items.value ['content'] AS content,
    items.value ['content'] ['metadata'] AS metadata,
    items.value ['creators'] AS creators,
    items.value ['grouping'][0]:group_value::string AS group_value,
    -- items.value ['interface'] :: STRING AS INTERFACE,
    -- items.value ['mutable'] :: boolean AS mutable,
    -- items.value ['ownership'] AS ownership,
    -- items.value ['royalty'] AS royalty,
    items.value ['supply'] AS supply,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__helius_nft_metadata') }},
    LATERAL FLATTEN(
        input => DATA :data :result
    ) AS items
    WHERE mint is not NULL
    and _inserted_timestamp <= '2023-10-13 05:35:36.367'

    qualify(ROW_NUMBER() over (PARTITION BY mint
ORDER BY
    _inserted_timestamp DESC)) = 1)

select 
    mint,
    authorities[0]:address::string as creator_address,
    metadata:attributes::array as metadata,
    content:links:image::string as image_url,
    content:json_uri::string as metadata_uri,
    supply:edition_nonce::int as edition,
    metadata:name::string as nft_name,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['mint']) }} AS nft_metadata_id,
    CASE 
        WHEN group_value IS NULL 
        THEN NULL 
        ELSE {{ dbt_utils.generate_surrogate_key(['group_value']) }} 
    END AS nft_collection_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
from pre_final


