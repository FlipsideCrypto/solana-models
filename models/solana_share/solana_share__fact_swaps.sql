{{ config(
    materialized = 'table',
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

  SELECT 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint,
    program_id,
    swap_program
FROM {{ref('defi__fact_swaps')}}
where block_timestamp::date between '2021-12-01' and '2021-12-31'