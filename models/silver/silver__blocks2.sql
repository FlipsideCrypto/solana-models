{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE','_inserted_date'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

SELECT 
    value :block_id :: INTEGER AS block_id,
    to_timestamp(data :blockTime) AS block_timestamp, 
    'mainnet' AS network, 
    'solana' AS chain_id, 
    data :blockHeight AS block_height, 
    TRIM(data :blockhash, '"') AS block_hash, 
    data :parentSlot AS previous_block_id, 
    TRIM(data :previousBlockhash, '"') AS previous_block_hash, 
    _inserted_date
FROM 
    {{ ref('bronze__blocks2') }}

WHERE 
  metadata IS NOT NULL
  AND (error IS NULL
  OR error :code <> '-32009' )-- Block is empty

{% if is_incremental() %}
AND
  _inserted_date >= (
    SELECT
      MAX(_inserted_date)
    FROM
      {{ this }}
  )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY block_id
ORDER BY
  _inserted_date DESC)) = 1