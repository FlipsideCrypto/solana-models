{{ config(
  materialized = 'view'
) }}


SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    program_id,
    platform,
    direction,
    user_address,
    amount,
    mint,
    _inserted_timestamp,
    bridge_mayan_transfers_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    {{ source('solana_silver', 'bridge_mayan_transfers') }}


