{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    -- succeeded,
    account as account_address,
    mint,
    post_owner as owner,
    pre_token_amount as pre_balance,
    post_token_amount as balance,
    token_balances_id as fact_token_balances_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp
FROM
    {{ ref('silver__token_balances') }}
