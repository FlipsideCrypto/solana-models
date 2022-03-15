{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}


WITH txs AS (
    SELECT 
        tx_id, 
        max(index) AS max_event_index
    FROM {{ ref('silver__events') }}
    WHERE program_id = 'M2mx93ekt1fmXSVkTrUL9xVFHkmME8HTUi5Cyc5aF7K' -- Magic Eden V2 Program ID

    {% if is_incremental() %}
      AND ingested_at::date >= getdate() - interval '2 days'
    {% endif %}

    GROUP BY tx_id HAVING count(tx_id) >= 2
),    
 
amounts_agg AS (
  SELECT
      block_timestamp, 
      block_id, 
      e.tx_id,
      e.index AS event_index, 
      i.index AS inner_index, 
      program_id,
      COALESCE(
          i.value :parsed :info :lamports, 
          0
      ) as amount, 
      i.value :parsed :info :mint :: STRING AS NFT, 
      i.value :parsed :info :wallet :: STRING AS wallet,
      max(inner_index) over (partition by e.tx_id) as max_inner_index,  
      ingested_at
  FROM {{ ref('silver__events') }} e

    INNER JOIN txs t
    ON t.tx_id = e.tx_id AND e.index = max_event_index 

    LEFT OUTER JOIN table(flatten(inner_instruction:instructions)) i 
  
  WHERE (amount IS NOT NULL
  OR NFT IS NOT NULL)

  {% if is_incremental() %}
      AND ingested_at::date >= getdate() - interval '2 days'
  {% endif %}
),   

sales_amount AS (
    SELECT 
        tx_id,
        sum(amount) AS amount
    FROM amounts_agg a
    WHERE inner_index <> max_inner_index
    AND NFT IS NULL

    GROUP BY tx_id
)

SELECT 
    block_timestamp, 
    block_id, 
    a.tx_id, 
    program_id,
    event_index, 
    s.amount / POW(10,9) as sales_amount, 
    NFT as mint, 
    wallet AS purchaser, 
    ingested_at
FROM amounts_agg a

INNER JOIN sales_amount s
ON s.tx_id = a.tx_id

WHERE WALLET IS NOT NULL 
AND NFT IS NOT NULL