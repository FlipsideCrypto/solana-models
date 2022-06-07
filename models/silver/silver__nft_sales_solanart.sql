{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, mint)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE'],
) }}

WITH sales_inner_instructions AS (
    SELECT 
        e.block_timestamp, 
        e.block_id, 
        e.tx_id, 
        t.succeeded, 
        e.program_id, 
        e.index, 
        COALESCE(
            i.value :parsed :info :lamports :: NUMBER, 
            0
        ) AS amount, 
        e.instruction :accounts [0] :: STRING AS purchaser, 
        e.instruction :accounts [1] :: STRING AS nft_account, 
        e.ingested_at 
    FROM {{ ref('silver__events') }} e
    INNER JOIN {{ ref('silver__transactions') }} t
    ON t.tx_id = e.tx_id
  
    LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
  
    WHERE e.program_id = 'CJsLwbP1iu5DuUikHEJnLfANgKy6stB2uFgvBBHoyxwz' -- Solanart Program ID 

    {% if is_incremental() %}
        AND e.ingested_at :: DATE >= CURRENT_DATE - 5
        AND t.ingested_at :: DATE >= CURRENT_DATE - 5
    {% endif %}
   
), 

post_token_balances AS (
    SELECT 
        tx_id, 
        account, 
        mint 
    FROM {{ ref('silver___post_token_balances') }} p 
    WHERE amount <> 0 -- Removes random account transfers with no NFT 

    {% if is_incremental() %}
        AND p.ingested_at :: DATE >= CURRENT_DATE - 5
    {% endif %}
)

SELECT 
    s.block_timestamp, 
    s.block_id, 
    s.tx_id, 
    s.succeeded, 
    s.program_id, 
    p.mint AS mint, 
    s.purchaser, 
    SUM(s.amount) / POW(10,9) AS sales_amount, 
    s.ingested_at
FROM sales_inner_instructions s

INNER JOIN post_token_balances p
ON s.tx_id = p.tx_id AND s.nft_account = p.account

GROUP BY s.block_timestamp, s.block_id, s.tx_id, s.succeeded, s.program_id, p.mint, s.purchaser, s.ingested_at

HAVING SUM(s.amount) > 0 -- Removes transfers