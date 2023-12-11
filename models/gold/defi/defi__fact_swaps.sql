{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}},
    tags = ['scheduled_non_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    l.address_name AS swap_program,
    COALESCE (
       swaps_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id','tx_id','program_id']
        ) }}
    ) AS fact_swaps_id,
    COALESCE(
        s.inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        s.modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__swaps') }}
    s
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    l
    ON s.program_id = l.address
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    l.address_name AS swap_program,
    swaps_intermediate_jupiterv6_id as fact_swaps_id,
    s.inserted_timestamp,
    s.modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv6') }}
    s
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    l
    ON s.program_id = l.address
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    l.address_name AS swap_program,
    swaps_intermediate_jupiterv5_id as fact_swaps_id,
    s.inserted_timestamp,
    s.modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv5_1_view') }}
    s
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    l
    ON s.program_id = l.address
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    l.address_name AS swap_program,
    swaps_intermediate_jupiterv5_id as fact_swaps_id,
    s.inserted_timestamp,
    s.modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv5_2_view') }}
    s
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    l
    ON s.program_id = l.address
