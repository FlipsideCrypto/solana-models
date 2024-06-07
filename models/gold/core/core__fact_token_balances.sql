{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

SELECT
    a.block_timestamp,
    a.block_id,
    a.tx_id,
    -- succeeded,
    a.account as account_address,
    a.mint,
    b.owner as owner,
    a.pre_token_amount as pre_balance,
    a.post_token_amount as balance,
    a.token_balances_id as fact_token_balances_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp
FROM
    {{ ref('silver__token_balances') }} a
left join {{ ref('core__fact_token_account_owners') }} b
on a.account = b.account_address
-- i dont think this is efficient to do every 30 mins
