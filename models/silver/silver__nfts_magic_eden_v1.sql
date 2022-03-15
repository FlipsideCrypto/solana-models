{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, mint)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'], 
) }}

WITH txs AS (
    SELECT 
        block_timestamp, 
        block_id, 
        tx_id, 
        program_id, 
        e.index, 
        COALESCE(
          i.value :parsed :info :lamports, 
          0
         ) AS amount, 
        instruction :accounts[1] :: STRING AS account, 
        instruction :accounts[0] :: STRING as owner, 
        ingested_at
    FROM {{ ref('silver__events') }} e, 
    table(flatten(inner_instruction:instructions)) i
   
    WHERE program_id = 'MEisE1HzehtrDpAAT8PnLHjpSSkRYakotTuJRPjTpo8' -- Magic Eden V1 Program ID 
    AND array_size(inner_instruction:instructions) > 2

    {% if is_incremental() %}
      AND ingested_at::date >= getdate() - interval '2 days'
    {% endif %}
    
),    

post_token_balances AS (
  SELECT 
    DISTINCT
      t.tx_id, 
      t.account, 
      p.mint
  FROM {{ ref('silver___post_token_balances') }} p

  INNER JOIN txs t
  ON p.tx_id = t.tx_id AND p.account = t.account

  {% if is_incremental() %}
    WHERE t.ingested_at::date >= getdate() - interval '2 days'
  {% endif %}
) 

SELECT 
    t.block_timestamp, 
    t.block_id, 
    t.tx_id, 
    t.program_id,
    sum(t.amount) / POW(10, 9) AS sales_amount, 
    p.mint AS mint, 
    t.owner AS purchaser, 
    t.ingested_at
FROM txs t

INNER JOIN post_token_balances p
ON p.tx_id = t.tx_id

GROUP BY t.block_timestamp, t.block_id, t.tx_id, t.program_id, p.mint, t.owner, t.ingested_at