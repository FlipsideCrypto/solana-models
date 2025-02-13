{{ config(
  materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    program_id,
    purchaser,
    seller,
    mint,
    sales_amount
FROM
 {{ source(
    'solana_silver',
    'nft_sales_coral_cube'
  ) }}