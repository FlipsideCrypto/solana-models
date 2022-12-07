{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}}
) }}

SELECT
    'jupiter aggregator v2' AS swap_program,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint
FROM
    {{ ref('silver__swaps_jupiter_dex') }}
UNION
SELECT
    'orca' AS swap_program,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt,
    from_mint,
    to_amt,
    to_mint
FROM
    {{ ref('silver__swaps_orca_dex') }}
UNION
SELECT
    'raydium v4' AS swap_program,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt,
    from_mint,
    to_amt,
    to_mint
FROM
    {{ ref('silver__swaps_raydium_dex') }}
UNION
SELECT
    'saber' AS swap_program,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt,
    from_mint,
    to_amt,
    to_mint
FROM
    {{ ref('silver__swaps_saber_dex') }}
