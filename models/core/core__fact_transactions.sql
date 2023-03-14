{{ config(
    materialized = 'view'
) }}

SELECT 
    block_timestamp,
    block_id,
    tx_id,
    recent_block_hash,
    signers,
    fee,
    succeeded,
    account_keys,
    pre_balances,
    post_balances,
    pre_token_balances,
    post_token_balances,
    instructions,
    inner_instructions,
    log_messages,
    units_consumed,
    units_limit
FROM
    {{ ref('silver__transactions') }}
