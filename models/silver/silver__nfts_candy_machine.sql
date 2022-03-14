{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'], 
) }}
    
SELECT 
    e.block_timestamp, 
    e.block_id, 
    e.tx_id, 
    program_id, 
    inner_instruction :instructions[0] :parsed :info :lamports / POW(10, 9) AS sales_amount,  
    p.mint AS mint, 
    inner_instruction :instructions[0] :parsed :info :authority :: STRING AS purchaser, 
    e.ingested_at
FROM {{ ref('silver__events') }} e
  
INNER JOIN {{ ref('silver___post_token_balances') }} p
ON e.tx_id = p.tx_id 
--AND inner_instruction :instructions[0] :parsed :info :authority :: STRING = p.account
    
WHERE program_id = 'cndyAnrLdpjq1Ssp1z8xxDsB8dxe7u4HL5Nxi2K5WXZ' 
AND e.index = 4

{% if is_incremental() %}
    AND e.ingested_at::date >= getdate() - interval '2 days'
{% endif %}