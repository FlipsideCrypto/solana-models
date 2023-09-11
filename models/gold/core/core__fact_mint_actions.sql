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
    token_account,
    decimal,
    mint_amount,
    mint_authority,
    signers
FROM
    {{ ref('silver__mint_actions') }}
