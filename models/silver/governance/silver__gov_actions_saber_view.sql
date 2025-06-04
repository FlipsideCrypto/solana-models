{{ config(
  materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    signer,
    locker_account,
    escrow_account,
    mint,
    action,
    amount,
    _inserted_timestamp,
    gov_actions_saber_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
  {{ source(
    'solana_silver',
    'gov_actions_saber'
  ) }}
