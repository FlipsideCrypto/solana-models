{{ config(
    materialized = 'view'
) }}

SELECT 
    'saber' as program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    signer,
    locker_account,
    mint,
    action,
    amount
FROM
    {{ ref('silver__gov_actions_saber') }}