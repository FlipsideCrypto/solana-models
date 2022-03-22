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
        e.ingested_at
    FROM {{ ref('silver__events') }} e
    
    INNER JOIN {{ ref('silver__transactions') }} t
    ON t.tx_id = e.tx_id 
  
    WHERE program_id = 'J7RagMKwSD5zJSbRQZU56ypHUtux8LRDkUpAPSKH4WPp' -- solana monke business marketplace

    {% if is_incremental() %}
        AND e.ingested_at :: DATE >= current_date - 2
        AND t.ingested_at :: DATE >= current_date - 2
    {% endif %}
), 

price AS (
    SELECT 
        b.tx_id, 
        p.amount 
    FROM base_table b
  
    INNER JOIN {{ ref('silver___post_token_balances') }} p 
    ON b.tx_id = p.tx_id 
    
    WHERE p.index = 0

    {% if is_incremental() %}
        AND p.ingested_at :: DATE >= current_date - 2
    {% endif %}
), 

monke AS (
    SELECT 
        DISTINCT
            b.tx_id, 
            p.mint
    FROM base_table b
  
    INNER JOIN {{ ref('silver___post_token_balances') }} p 
    ON b.tx_id = p.tx_id 
    
    WHERE (p.index = 2
    OR p.index = 1) 
    AND p.mint <> 'So11111111111111111111111111111111111111112'

    {% if is_incremental() %}
        AND p.ingested_at :: DATE >= current_date - 2
    {% endif %}
)

SELECT 
     b.block_timestamp, 
     b.block_id, 
     b.tx_id, 
     b.succeeded, 
     b.program_id, 
     m.mint AS mint, 
     b.acct_1 AS purchaser, 
     p.amount / POW(10,9) AS sales_amount, 
     b.ingested_at
FROM base_table b

INNER JOIN price p
ON b.tx_id = p.tx_id

INNER JOIN monke m
ON b.tx_id = m.tx_id 

WHERE amount <> 0 -- To ignore internal wallet transfers on the marketplace
