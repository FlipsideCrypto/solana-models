{{ config(
  materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    program_id,
    tx_id,
    index,
    inner_index,
    swap_index,
    succeeded,
    swapper,
    from_amt,
    from_mint,
    to_amt,
    to_mint,
    amount_in,
    min_amount_out,
    _inserted_timestamp,
    swaps_intermediate_lifinity_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
  {{ source(
    'solana_silver',
    'swaps_intermediate_lifinity'
  ) }}
