{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
) }}

WITH offchain AS (

    SELECT
        items.value,
        items.value ['id'] :: STRING AS mint,
        0.000005 AS mint_price,
        'So11111111111111111111111111111111111111111' AS mint_currency,
        'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY' AS program_id,
        page,
        _inserted_timestamp,
        nft_collection_mint,
        items.seq AS seq
    FROM
        solana_dev.bronze_api.helius_compressed_nfts,
        LATERAL FLATTEN(
            input => DATA :data [0] :result :items
        ) AS items

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY mint
ORDER BY
    _inserted_timestamp DESC)) = 1
),
offchain_ordered AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY nft_collection_mint
            ORDER BY
                page,
                seq
        ) AS ROW_NUMBER
    FROM
        offchain
),
onchain AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY collection_mint
            ORDER BY
                block_timestamp DESC
        ) AS ROW_NUMBER
    FROM
        {{ ref('silver__nft_compressed_mints_onchain') }}

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    A.block_timestamp,
    A.block_id,
    A.succeeded,
    A.tx_id,
    A.leaf_owner,
    A.collection_mint,
    A._inserted_timestamp,
    A.creator_address AS purchaser,
    b.mint,
    b.mint_price,
    b.mint_currency,
    b.program_id
FROM
    onchain A
    LEFT JOIN offchain_ordered b
    ON A.row_number = b.row_number
    AND A.collection_mint = b.nft_collection_mint
WHERE
    b.row_number IS NOT NULL
