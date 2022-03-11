{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH txs AS (
    SELECT 
        tx_id, 
        count(tx_id) AS ct
    FROM {{ ref('silver__events') }}
    WHERE program_id = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' -- Magic Eden V2 Program ID
    
    {% if is_incremental() %}
      AND ingested_at::date >= getdate() - interval '2 days'
    {% endif %}

    GROUP BY tx_id HAVING count(tx_id) >= 2
),  

last_index AS (
  SELECT
      e.block_timestamp, 
      e.block_id, 
      e.tx_id,
      t.ct, 
      e.index, 
      e.program_id,
      instruction, 
      inner_instruction, 
      e.ingested_at
  FROM txs t

  INNER JOIN {{ ref('silver__events') }} e
  ON t.tx_id = e.tx_id

  WHERE e.index = t.ct - 1

  {% if is_incremental() %}
      AND ingested_at::date >= getdate() - interval '2 days'
  {% endif %}
),  

amounts_agg AS (
  SELECT
      block_timestamp, 
      block_id, 
      tx_id,
      i.index AS index, 
      program_id,
      i.value :parsed :info :lamports / POW(10,9) as amount, 
      i.value :parsed :info :mint :: STRING AS NFT, 
      i.value :parsed :info :wallet :: STRING AS wallet, 
      ingested_at
  FROM last_index, 
  table(flatten(inner_instruction:instructions)) i 
  
  WHERE amount IS NOT NULL 
  OR wallet IS NOT NULL
),  

max_index_remove AS (
  SELECT
        tx_id, 
        max(index) AS max_index
  FROM amounts_agg
  GROUP BY tx_id
), 

NFT AS (
    SELECT 
        tx_id, 
        NFT, 
        wallet
    FROM amounts_agg
    WHERE NFT IS NOT NULL 
    AND wallet IS NOT NULL
), 

amounts AS (
    SELECT 
      block_timestamp, 
      block_id, 
      a.tx_id,
      index,
      program_id,
      amount,  
      ingested_at
    FROM amounts_agg a
    
    INNER JOIN max_index_remove m
    ON m.tx_id = a.tx_id
  
    WHERE index <> m.max_index   
),  

sales_amount AS (
    SELECT 
        tx_id, 
        sum(amount) AS sales_amount
    FROM amounts
    GROUP BY tx_id
)

SELECT 
    block_timestamp, 
    block_id, 
    a.tx_id, 
    program_id,
    s.sales_amount, 
    n.NFT, 
    n.wallet AS purchaser, 
    ingested_at
FROM amounts a

INNER JOIN NFT n
ON n.tx_id = a.tx_id

INNER JOIN sales_amount s
ON s.tx_id = a.tx_id

WHERE index = 0