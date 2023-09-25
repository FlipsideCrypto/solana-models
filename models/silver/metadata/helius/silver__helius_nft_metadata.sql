{{ config(
    materialized = 'incremental',
    unique_key = "mint",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['helius']
) }}

SELECT
    items.value ['id'] :: STRING AS mint,
    items.value ['authorities'] AS authorities,
    items.value ['burnt']::boolean AS burnt,
    items.value ['compression'] AS compression,
    items.value ['content'] AS content,
    items.value ['content'] ['metadata'] AS metadata,
    items.value ['creators'] AS creators,
    items.value ['grouping'] AS GROUPING,
    items.value ['interface'] :: STRING AS INTERFACE,
    items.value ['mutable'] :: boolean AS mutable,
    items.value ['ownership'] AS ownership,
    items.value ['royalty'] AS royalty,
    items.value ['supply'] AS supply,
    _inserted_timestamp
FROM
    {{ ref('bronze__helius_nft_metadata') }},
    LATERAL FLATTEN(
        input => DATA :data [0] :result
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
    _inserted_timestamp DESC)) = 1

