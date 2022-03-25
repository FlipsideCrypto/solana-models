{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, NFT, mint_currency)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'], 
) }}
    
WITH mint_tx AS (
    SELECT 
        DISTINCT t.tx_id, 
        t.succeeded
    FROM {{ ref('silver__events') }} e 
    
    INNER JOIN {{ ref('silver__transactions') }} t 
    ON t.tx_id = e.tx_id 

    WHERE event_type = 'mintTo'

    {% if is_incremental() %}
        AND e.ingested_at :: DATE >= current_date - 2
        AND t.ingested_at :: DATE >= current_date - 2
    {% endif %}
), 

txs AS (
   SELECT
      e.block_timestamp, 
      e.block_id, 
      t.tx_id,
      t.succeeded AS succeeded,   
      program_id, 
      COALESCE(
          i.value :parsed :info :lamports :: INTEGER, 
          i.value :parsed :info :amount :: INTEGER
      ) AS sales_amount,
      i.index AS inner_index, 
      i.value :parsed :info :mint :: STRING AS NFT, 
      i.value :parsed :info :multisigAuthority :: STRING AS wallet, 
      i.value :parsed :info :authority :: STRING AS authority, 
      i.value :parsed :info :source :: STRING AS source, 
      i.value: parsed :info :destination :: STRING AS destination,
      e.ingested_at
   FROM mint_tx t

   INNER JOIN {{ ref('silver__events') }} e 
   ON t.tx_id = e.tx_id 

   LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i

   WHERE e.event_type IS NULL
   AND array_contains('metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'::variant, e.instruction:accounts::ARRAY)
   AND t.succeeded = TRUE

    {% if is_incremental() %}
        AND e.ingested_at :: DATE >= current_date - 2
    {% endif %}
),   

mint_currency AS (
SELECT 
  DISTINCT
    t.tx_id,   
    p.mint, 
    p.account, 
    p.decimal    
FROM txs t

INNER JOIN {{ ref('silver___post_token_balances') }} p
ON t.tx_id = p.tx_id 

WHERE source = p.account 

{% if is_incremental() %}
    AND p.ingested_at :: DATE >= current_date - 2
{% endif %}
), 

wallet AS (
    SELECT 
        DISTINCT
          tx_id, 
          wallet, 
          NFT
    FROM txs
    WHERE wallet IS NOT NULL
)

SELECT 
    block_timestamp, 
    block_id, 
    t.tx_id, 
    succeeded, 
    program_id,
    w.wallet AS purchaser,  
    max(sales_amount / POW(10, COALESCE(p.decimal, 9))) AS mint_price,
    COALESCE(
      p.mint,
      'So11111111111111111111111111111111111111111'
    ) AS mint_currency, 
    w.NFT AS NFT, 
    ingested_at
FROM txs t

LEFT OUTER JOIN mint_currency p 
ON p.tx_id = t.tx_id AND p.account = t.source

INNER JOIN wallet w
ON t.tx_id = w.tx_id 

GROUP BY block_timestamp, block_id, t.tx_id, program_id, w.wallet, succeeded, p.mint, w.NFT, ingested_at