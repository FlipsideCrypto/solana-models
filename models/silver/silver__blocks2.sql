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
    tx_count, 
    data :blockHeight AS block_height, 
    TRIM(data :blockhash, '"') AS block_hash, 
    data :parentSlot AS previous_block_id, 
    TRIM(data :previousBlockhash, '"') AS previous_block_hash, 
    _inserted_date
FROM 
    {{ source(
        'solana_external', 
        'blocks_api'
    ) }}
    b

LEFT OUTER JOIN tx_counter 
t 
ON b.block_id = t.block_id

{% if is_incremental() %}
WHERE
  _inserted_date >= (
    SELECT
      MAX(_inserted_date)
    FROM
      {{ this }}
  )
{% endif %}

qualify(ROW_NUMBER() over(PARTITION BY b.block_id
ORDER BY
  _inserted_date DESC)) = 1