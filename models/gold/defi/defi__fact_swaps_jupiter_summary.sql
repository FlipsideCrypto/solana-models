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
    NULL as is_dca_swap,
    NULL as dca_requester,
    NULL as is_limit_swap,
    NULL as limit_requester,
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
    is_dca_swap,
    dca_requester,
    is_limit_swap,
    limit_requester,
    swaps_intermediate_jupiterv6_id AS fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__swaps_intermediate_jupiterv6_2') }}
WHERE
    block_timestamp::date >= '2023-08-03'
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    swap_index as index,
    NULL as inner_index,
    row_number() OVER (PARTITION BY tx_id ORDER BY index)-1 AS swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amt AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amt AS swap_to_amount,
    program_id,
    NULL as is_dca_swap,
    NULL as dca_requester,
    NULL as is_limit_swap,
    NULL as limit_requester,
    swaps_intermediate_jupiterv5_id as fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv5_1_view') }}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    swap_index as index,
    NULL as inner_index,
    row_number() OVER (PARTITION BY tx_id ORDER BY index)-1 AS swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amt AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amt AS swap_to_amount,
    program_id,
    NULL as is_dca_swap,
    NULL as dca_requester,
    NULL as is_limit_swap,
    NULL as limit_requester,
    swaps_intermediate_jupiterv5_id as fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv5_2_view') }}
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
    NULL as is_dca_swap,
    NULL as dca_requester,
    NULL as is_limit_swap,
    NULL as limit_requester,
    swaps_intermediate_jupiterv4_id as fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv4_2') }}
WHERE
    block_timestamp::date > '2023-10-31'
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    NULL as index,
    NULL as inner_index,
    0 as swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amt AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amt AS swap_to_amount,
    program_id,
    NULL as is_dca_swap,
    NULL as dca_requester,
    NULL as is_limit_swap,
    NULL as limit_requester,
    swaps_id as fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps') }}
WHERE
    program_id = 'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB'
    and block_timestamp::date <= '2023-10-31'
    and succeeded
