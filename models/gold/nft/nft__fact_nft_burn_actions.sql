{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' }}},
    tags = ['scheduled_non_core']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    index,
    inner_index,
    event_type,
    mint,
    burn_amount,
    burn_authority,
    signers,
    DECIMAL,
    MINT_STANDARD_TYPE,
    COALESCE (
        nft_burn_actions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id', 'index', 'inner_index', 'mint']
        ) }}
    ) AS fact_nft_burn_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_burn_actions') }}
