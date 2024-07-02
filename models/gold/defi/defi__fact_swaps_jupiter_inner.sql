{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}},
    tags = ['scheduled_non_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    index,
    inner_index,
    swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amount AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amount AS swap_to_amount,
    swap_program_id,
    aggregator_program_id,
    swaps_inner_intermediate_jupiterv6_id AS fact_swaps_jupiter_inner_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__swaps_inner_intermediate_jupiterv6') }}