{{ config(
  materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    program_id,
    tx_id,
    succeeded,
    swap_index,
    index,
    inner_index,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint,
    _inserted_timestamp,
    swaps_intermediate_saber_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
  {{ source(
    'solana_silver',
    'swaps_intermediate_saber'
  ) }}
