{{ config(
  materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    index,
    inner_index,
    mint,
    purchaser,
    seller, 
    sales_amount,
    _inserted_timestamp,
    nft_sales_solanart_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
  {{ source(
    'solana_silver',
    'nft_sales_solanart'
  ) }}
