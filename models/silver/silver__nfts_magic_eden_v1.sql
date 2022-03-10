{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, NFT)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH txs AS (
  SELECT 
      block_timestamp, 
      block_id, 
      tx_id, 
      index, 
      program_id, 
      instruction :accounts[0] :: STRING AS purchaser,  
       COALESCE (inner_instruction:instructions[0]:parsed:info:lamports/POW(10,9), 0) as price_0, 
       COALESCE (inner_instruction:instructions[1]:parsed:info:lamports/POW(10,9), 0) as price_1,
       COALESCE (inner_instruction:instructions[2]:parsed:info:lamports/POW(10,9), 0) as price_2, 
       COALESCE (inner_instruction:instructions[3]:parsed:info:lamports/POW(10,9), 0) as price_3, 
      COALESCE (inner_instruction:instructions[4]:parsed:info:lamports/POW(10,9), 0) as price_4, 
      COALESCE (inner_instruction:instructions[5]:parsed:info:lamports/POW(10,9), 0) as price_5,
      (price_0 + price_1 + price_2 + price_3 + price_4 + price_5) AS sale_amount,  
      ingested_at
  FROM "SOLANA_DEV"."SILVER"."EVENTS"

  WHERE program_id = 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8' -- Magic Eden V1 Program ID
  AND array_size(inner_instruction:instructions) > 2

  {% if is_incremental() %}
      AND ingested_at >= getdate() - interval '2 days'
  {% endif %}
)

SELECT 
    t.block_timestamp, 
    t.block_id, 
    t.tx_id, 
    t.index, 
    t.program_id, 
    purchaser, 
    p.mint AS NFT, 
    sale_amount, 
    t.ingested_at
FROM txs t

INNER JOIN "SOLANA_DEV"."SILVER"."_POST_TOKEN_BALANCES" p
ON t.tx_id = p.tx_id AND t.index = p.index