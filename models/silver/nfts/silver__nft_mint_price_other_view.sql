{{ config(
  materialized = 'view'
) }}

SELECT 
    mint,
    payer,
    mint_currency,
    decimal,
    program_id,
    mint_price,
    tx_ids,
    block_timestamp,
    _inserted_timestamp
FROM
  {{ source(
    'solana_silver',
    'nft_mint_price_other'
  ) }}