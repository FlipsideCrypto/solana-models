{{ config(
  materialized = 'view'
) }}

SELECT
    block_id,
    block_timestamp,
    program_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    purchaser,
    seller,
    tree_authority,
    merkle_tree,
    leaf_index,
    mint,
    mint_inserted_timestamp,
    sales_amount,
    _inserted_timestamp,
    nft_sales_solsniper_cnft_id,
    inserted_timestamp,
    modified_timestamp
FROM
  {{ source(
    'solana_silver',
    'nft_sales_solsniper_cnft'
  ) }}