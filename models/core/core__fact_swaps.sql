{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} }
) }}

SELECT
    swap_program,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint
FROM
    {{ ref('silver__swaps') }}
