{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    tags = ['compressed_nft']
) }}

WITH offchain AS (

    SELECT
        r.value:tx_id::string as tx_id,
        coalesce(r.value:mint::string,'') AS mint,
        0.000005 AS mint_price,
        'So11111111111111111111111111111111111111111' AS mint_currency,
        'BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY' AS program_id,
        start_inserted_timestamp as _inserted_timestamp
    FROM
        {{ ref('bronze_api__parse_compressed_nft_mints')}},
        TABLE(FLATTEN(responses)) AS r
    WHERE 
        mint <> ''
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
onchain AS (
    SELECT
        *
    FROM
        {{ ref('silver__nft_compressed_mints_onchain') }}

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
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
    b.tx_id,
    A.leaf_owner,
    A.collection_mint,
    b._inserted_timestamp,
    A.creator_address AS purchaser,
    b.mint,
    b.mint_price,
    b.mint_currency,
    b.program_id,
    {{ dbt_utils.generate_surrogate_key(
        ['b.tx_id']
    ) }} AS nft_compressed_mints_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    offchain b
    LEFT JOIN onchain A
    ON A.tx_id = b.tx_id
