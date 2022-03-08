{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE'],
) }}

WITH postTokenBalances AS (
    SELECT 
        tx_id, 
        index,
        account_index,
        mint, 
        owner, 
        decimal
    FROM {{ ref('silver___post_token_balances') }}  
), 

base_spl AS (
    SELECT 
        e.block_id,
        e.block_timestamp, 
        post.tx_id,
        e.index, 
        post.account_index,
        post.mint, 
        post.owner,  
        COALESCE(
          e.instruction :parsed :info :tokenAmount : decimals, 
          post.decimal
        ) AS decimal, 
        COALESCE(
          e.instruction :parsed :info :amount :: INTEGER, 
          e.instruction :parsed :info :tokenAmount :amount :: INTEGER  
        ) AS amount, 
        e.instruction :parsed :info :authority :: STRING AS authority, 
        e.ingested_at
    FROM postTokenBalances post 
    
    LEFT OUTER JOIN {{ ref('silver__events') }} e 
    ON e.tx_id = post.tx_id 

    WHERE e.event_type IN ('transfer', 'transferChecked') 
    AND e.instruction :parsed :info :authority :: STRING IS NOT NULL

    {% if is_incremental() %}
        AND ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
),

base_sol AS (
    SELECT 
        block_id,
        block_timestamp, 
        tx_id,
        index, 
        instruction :parsed :info :source :: STRING AS tx_from, 
        instruction :parsed :info :destination :: STRING AS tx_to, 
        instruction :parsed :info :lamports / POW(10,9) AS amount,
        'So11111111111111111111111111111111111111112' AS mint,
        ingested_at 

    FROM {{ ref('silver__events') }} 
    
    WHERE event_type IN ('transfer', 'transferChecked') 
    AND instruction :parsed :info :lamports :: STRING IS NOT NULL  

    {% if is_incremental() %}
        AND ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
), 

sender AS (
    SELECT 
        tx_id, 
        account_index, 
        CASE 
            WHEN owner = authority THEN owner
            ELSE authority 
        END AS tx_from 
    FROM base_spl t       
), 

receiver AS (
    SELECT 
        tx_id,
        account_index, 
        owner AS tx_to
    FROM base_spl t
    WHERE owner <> authority
), 

spl_token AS (
  SELECT 
       block_id,
       block_timestamp, 
       t.tx_id,
       t.index, 
       t.account_index, 
       s.tx_from, 
       r.tx_to, 
       t.decimal, 
       amount / POW(10, t.decimal) AS amount,
       mint,
       ingested_at
  FROM base_spl t

  LEFT OUTER JOIN sender s
  ON t.tx_id = s.tx_id 

  LEFT OUTER JOIN receiver r
  ON t.tx_id = r.tx_id AND t.account_index = r.account_index

  WHERE tx_to IS NOT NULL
)

SELECT 
    block_id, 
    block_timestamp, 
    tx_id, 
    index, 
    tx_from, 
    tx_to, 
    amount, 
    mint, 
    ingested_at 

FROM base_sol

UNION ALL 

SELECT 
    block_id, 
    block_timestamp, 
    tx_id, 
    index, 
    tx_from, 
    tx_to, 
    amount, 
    mint, 
    ingested_at
    
FROM spl_token

qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id, index
ORDER BY
  ingested_at DESC)) = 1