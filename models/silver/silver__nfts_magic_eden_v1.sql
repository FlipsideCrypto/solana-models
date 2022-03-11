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
        program_id,  
        i.value :parsed :info :lamports / POW(10,9) AS amount, 
        i.value :parsed :info :account :: STRING AS account, 
        i.value :parsed :info :newAuthority :: STRING as owner, 
        inner_instruction, 
        ingested_at
    FROM "SOLANA_DEV"."SILVER"."EVENTS", 
    table(flatten(inner_instruction:instructions)) i
   
    WHERE program_id = 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8' -- Magic Eden V1 Program ID 
    AND array_size(inner_instruction:instructions) > 2

    {% if is_incremental() %}
      AND ingested_at::date >= getdate() - interval '2 days'
    {% endif %}
    
),   

sales_amount AS (
  SELECT 
    tx_id, 
    sum(amount) AS sales_amount
  FROM txs 
  GROUP BY tx_id
) 

SELECT 
    t.block_timestamp, 
    t.block_id, 
    t.tx_id, 
    t.program_id, 
    s.sales_amount,
    p.mint AS NFT, 
    t.owner AS purchaser, 
    t.ingested_at
FROM txs t

INNER JOIN sales_amount s
ON s.tx_id = t.tx_id

INNER JOIN "SOLANA_DEV"."SILVER"."_POST_TOKEN_BALANCES" p
ON p.tx_id = t.tx_id AND p.account = t.account

WHERE t.account IS NOT NULL 