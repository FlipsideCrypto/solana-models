{{ config(
    materialized = 'incremental',
    unique_key = "mint",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
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
    items.value ['interface'] AS INTERFACE,
    items.value ['mutable'] AS mutable,
    items.value ['ownership'] AS ownership,
    items.value ['royalty'] AS royalty,
    items.value ['supply'] AS supply,
    _inserted_timestamp
FROM
    {{ ref('bronze__helius_nft_metadata') }},
    LATERAL FLATTEN(
        input => DATA :data [0] :result
    ) AS items
{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
