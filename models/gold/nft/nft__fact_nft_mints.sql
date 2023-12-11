{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    tags = ['scheduled_non_core']
) }}

SELECT
    block_timestamp,
    block_id,
    initialization_tx_id AS tx_id,
    succeeded,
    program_id,
    purchaser,
    mint_price,
    mint_currency,
    mint,
    FALSE AS is_compressed,
    COALESCE (
        nft_mints_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id', 'mint', 'purchaser', 'mint_currency']
        ) }}
    ) AS fact_nft_mints_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_mints') }}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    purchaser,
    mint_price,
    mint_currency,
    mint,
    TRUE AS is_compressed,
    COALESCE (
        nft_compressed_mints_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_nft_mints_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_compressed_mints') }}
