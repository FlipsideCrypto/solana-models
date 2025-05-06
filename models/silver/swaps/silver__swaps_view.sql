{{ config(
  materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    program_id,
    tx_id,
    succeeded,
    swapper,
    from_amt,
    from_mint,
    to_amt,
    to_mint,
    _log_id,
    _inserted_timestamp,
    swaps_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
  {{ source(
    'solana_silver',
    'swaps'
  ) }}
