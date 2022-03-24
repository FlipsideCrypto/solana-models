{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'], 
) }}
    
WITH txs AS (
   SELECT
      e.block_timestamp, 
      e.block_id, 
      e.tx_id,
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
   FROM {{ ref('silver__events') }} e 

   INNER JOIN {{ ref('silver__transactions') }} t 
   ON e.tx_id = t.tx_id 

   LEFT OUTER JOIN TABLE(FLATTEN(inner_instruction :instructions)) i

   WHERE (program_id = 'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ' 
   OR program_id = 'cndy3Z4yapfJBmL3ShUp5exZKqR3z33thTzeNMm2gRZ')
   AND e.index > 0
   AND (authority IS NOT NULL 
   OR NFT IS NOT NULL
   OR i.index = 0) 
   AND t.succeeded = TRUE

   {% if is_incremental() %}
    AND e.ingested_at :: DATE >= current_date - 2
    AND t.ingested_at :: DATE >= current_date - 2
   {% endif %}
),   

mint_currency AS (
SELECT 
  DISTINCT
    t.tx_id,  
    t.authority, 
    p.mint, 
    p.decimal    
FROM txs t

INNER JOIN {{ ref('silver___post_token_balances') }} p
ON t.tx_id = p.tx_id 

WHERE source = p.account 

{% if is_incremental() %}
    AND p.ingested_at :: DATE >= current_date - 2
{% endif %}
)       

SELECT 
    block_timestamp, 
    block_id, 
    t.tx_id, 
    program_id,
    max(wallet) AS purchaser,  
    max(sales_amount / POW(10, COALESCE(p.decimal, 9))) AS mint_price,
    COALESCE(
      p.mint,
      'So11111111111111111111111111111111111111111'
    ) AS mint_currency, 
    max(NFT) AS NFT, 
    ingested_at
FROM txs t

LEFT OUTER JOIN mint_currency p 
ON t.tx_id = p.tx_id

GROUP BY block_timestamp, block_id, t.tx_id, program_id, succeeded, p.mint, ingested_at