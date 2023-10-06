{{ config(
    materialized = 'view'
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
    MIN_STANDARD_TYPE
FROM
    {{ ref('silver__nft_burn_actions') }}
