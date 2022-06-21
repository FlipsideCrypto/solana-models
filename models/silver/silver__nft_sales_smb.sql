{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

WITH base_table AS (
    SELECT 
        e.block_timestamp, 
        e.block_id, 
        e.tx_id, 
        t.succeeded, 
        e.program_id, 
        instruction :accounts[0] :: STRING AS acct_1, 
        instruction :accounts[1] :: STRING AS mint, 
        e.ingested_at,
        e._inserted_timestamp
    FROM {{ ref('silver__events') }} e
    
    INNER JOIN {{ ref('silver__transactions') }} t
    ON t.tx_id = e.tx_id 
  
    WHERE program_id = 'J7RagMKwSD5zJSbRQZU56ypHUtux8LRDkUpAPSKH4WPp' -- solana monke business marketplace

    {% if is_incremental() %}
        AND e.ingested_at :: DATE >= CURRENT_DATE - 2
        AND t.ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
), 

price AS (
    SELECT 
        b.tx_id, 
        e.instruction :parsed :info :lamports :: NUMBER AS amount 
    FROM {{ ref('silver__events') }} e
  
    INNER JOIN base_table b 
    ON e.tx_id = b.tx_id 
    
    WHERE e.event_type = 'transfer'

    {% if is_incremental() %}
        AND b.ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
) 

SELECT 
     b.block_timestamp, 
     b.block_id, 
     b.tx_id, 
     b.succeeded, 
     b.program_id, 
     b.mint, 
     b.acct_1 AS purchaser, 
     p.amount / POW(10,9) AS sales_amount, 
     b.ingested_at,
     b._inserted_timestamp
FROM base_table b

INNER JOIN price p
ON b.tx_id = p.tx_id

WHERE p.amount <> 0 -- To ignore internal wallet transfers on the marketplace
AND b.mint <> 'So11111111111111111111111111111111111111112'