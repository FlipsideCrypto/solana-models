{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}},
    tags = ['scheduled_non_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    swap_index AS index,
    NULL AS inner_index,
    swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amt AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amt AS swap_to_amount,
    program_id,
    swaps_intermediate_jupiterv6_id AS fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__swaps_intermediate_jupiterv6_view') }}
WHERE
    block_timestamp::date < '2023-08-03'
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
    program_id,
    swaps_intermediate_jupiterv6_id AS fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__swaps_intermediate_jupiterv6_2') }}