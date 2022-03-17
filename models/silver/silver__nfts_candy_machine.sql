{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'], 
) }}
    
WITH txs AS (
    SELECT
      block_timestamp, 
      block_id, 
      tx_id,   
      program_id, 
      COALESCE(
          inner_instruction :instructions[0] :parsed :info :lamports :: INTEGER, -- Move this to the last step 
          inner_instruction :instructions[0] :parsed :info :amount :: INTEGER
      ) AS sales_amount,
      inner_instruction :instructions[0] :parsed :info :authority :: STRING AS authority, -- Can we pull this from the instructions account array? 
      inner_instruction :instructions[0] :parsed :info :source :: STRING AS source, 
      inner_instruction :instructions[0] :parsed :info :destination :: STRING AS destination,
      ingested_at
      FROM {{ ref('silver__events') }} 

      WHERE program_id = 'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ' 
      AND index > 0

    {% if is_incremental() %}
      AND ingested_at::date >= getdate() - interval '2 days'
    {% endif %}
    
),      

mint_currency AS (
    SELECT 
    DISTINCT
        t.tx_id,  
        CASE WHEN source = p.account THEN p.mint 
        ELSE 'So11111111111111111111111111111111111111111' END AS mint_currency, 
        CASE WHEN source = p.account THEN p.decimal 
        ELSE 9 END AS decimal 
    FROM txs t

    INNER JOIN {{ ref('silver___post_token_balances') }} p
    ON t.tx_id = p.tx_id

  {% if is_incremental() %}
      WHERE t.ingested_at::date >= getdate() - interval '2 days'
  {% endif %}
),     

NFT as (
    SELECT
        DISTINCT
        t.tx_id,  
        p.mint AS NFT 
    FROM txs t

    INNER JOIN {{ ref('silver___post_token_balances') }} p
    ON t.tx_id = p.tx_id 
    AND source <> p.account  

  {% if is_incremental() %}
      WHERE t.ingested_at::date >= getdate() - interval '2 days'
  {% endif %}
)

SELECT 
    block_timestamp, 
    block_id, 
    t.tx_id, 
    program_id,
    COALESCE(
        source, 
        authority
     ) AS purchaser, 
    sum(sales_amount) / POW(10, p.decimal) AS sales_amount,
    p.mint_currency, 
    n.NFT, 
    ingested_at
FROM txs t

INNER JOIN NFT n 
ON t.tx_id = n.tx_id  

INNER JOIN mint_currency p 
ON t.tx_id = p.tx_id

GROUP BY block_timestamp, block_id, t.tx_id, program_id, purchaser, p.decimal, p.mint_currency, n.NFT, ingested_at