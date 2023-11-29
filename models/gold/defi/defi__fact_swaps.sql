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
    _log_id
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
    concat_ws('-',tx_id,swap_index) as _log_id
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
    concat_ws('-',tx_id,swap_index) as _log_id
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
    concat_ws('-',tx_id,swap_index) as _log_id
FROM
    {{ ref('silver__swaps_intermediate_jupiterv5_2_view') }}
    s
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    l
    ON s.program_id = l.address
