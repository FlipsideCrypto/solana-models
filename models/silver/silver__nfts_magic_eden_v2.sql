{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}


WITH txs AS (
    SELECT 
        tx_id
    FROM {{ ref('silver__events') }}
    WHERE program_id = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' -- Magic Eden V2 Program ID

    {% if is_incremental() %}
      AND ingested_at::date >= getdate() - interval '2 days'
    {% endif %}

    GROUP BY tx_id HAVING count(tx_id) = 3
),    
 
amounts_agg AS (
  SELECT
      block_timestamp, 
      block_id, 
      tx_id,
      e.index AS event_index, 
      i.index AS inner_index, 
      program_id,
      i.value :parsed :info :lamports / POW(10,9) as amount, 
      i.value :parsed :info :mint :: STRING AS NFT, 
      i.value :parsed :info :wallet :: STRING AS wallet,
      max(inner_index) over (partition by tx_id) as max_inner_index, 
      ingested_at
  FROM {{ ref('silver__events') }} e, 
  table(flatten(inner_instruction:instructions)) i 
  
  WHERE amount IS NOT NULL 

  {% if is_incremental() %}
      AND ingested_at::date >= getdate() - interval '2 days'
  {% endif %}

  OR wallet IS NOT NULL
),   

sales_amount AS (
    SELECT 
        tx_id,
        event_index, 
        amount
    FROM amounts_agg
    WHERE event_index = 0 
)

SELECT 
    block_timestamp, 
    block_id, 
    a.tx_id, 
    program_id,
    s.amount as sales_amount, 
    NFT as mint, 
    wallet AS purchaser, 
    ingested_at
FROM amounts_agg a

INNER JOIN sales_amount s
ON s.tx_id = a.tx_id

WHERE program_id = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' 
AND NFT IS NOT NULL 
AND purchaser IS NOT NULL