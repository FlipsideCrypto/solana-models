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
    modified_timestamp
FROM
    {{ ref('silver__swaps_inner_intermediate_jupiterv6') }}
UNION ALL
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
    swaps_inner_intermediate_jupiterv5_id AS fact_swaps_jupiter_inner_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_inner_intermediate_jupiterv5') }}
UNION ALL
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
    swaps_inner_intermediate_jupiterv4_id AS fact_swaps_jupiter_inner_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_inner_intermediate_jupiterv4') }}
WHERE 
    block_timestamp > '2023-10-31'
UNION ALL
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
    from_amt AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amt AS swap_to_amount,
    inner_swap_program_id AS swap_program_id,
    program_id AS aggregator_program_id,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','inner_index']) }} AS fact_swaps_jupiter_inner_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv4_view') }}
WHERE
    block_timestamp <= '2023-10-31'
    and succeeded
