{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    account as account_address,
    mint,
    account as owner,
    pre_amount as pre_balance,
    post_amount as balance,
    sol_balances_id as fact_sol_balances_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp
FROM
    {{ ref('silver__sol_balances') }}
