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
      AND ingested_at >= getdate() - interval '2 days'
    {% endif %}
    
    GROUP BY tx_id
) 

SELECT
    e.block_timestamp, 
    e.block_id, 
    e.tx_id,
    t.ct, 
    e.index, 
    e.program_id,
    instruction :accounts[0] :: STRING AS purchaser, 
    p.mint AS NFT, 
    inner_instruction :instructions[0] :parsed :info :lamports / POW(10,9) :: INTEGER AS sale_amount, 
    e.ingested_at
FROM txs t

INNER JOIN {{ ref('silver__events') }} e
ON t.tx_id = e.tx_id 

INNER JOIN {{ ref('silver___post_token_balances') }} p
ON e.tx_id = p.tx_id AND e.index = p.index 

WHERE t.ct = 3 
AND e.index = 0