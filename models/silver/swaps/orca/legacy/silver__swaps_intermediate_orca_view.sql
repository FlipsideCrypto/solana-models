{{ config(
  materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    program_id,
    index,
    inner_index,
    swapper,
    from_mint,
    from_amt,
    to_mint,
    to_amt,
    _inserted_timestamp,
    swap_index,
    _log_id
FROM
  {{ source(
    'solana_silver',
    'swaps_intermediate_orca'
  ) }}
