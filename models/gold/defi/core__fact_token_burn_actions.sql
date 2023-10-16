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
    mint_standard_type

FROM
    {{ ref('silver__token_burn_actions') }}
