{{ config(
  materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    swap_index,
    program_id,
    succeeded,
    swapper,
    from_amt,
    from_mint,
    to_amt,
    to_mint,
    _inserted_timestamp,
    swaps_intermediate_jupiterv6_id,
    inserted_timestamp,
    modified_timestamp,
    invocation_id
FROM
  {{ source('solana_silver', 'swaps_intermediate_jupiterv6') }}
