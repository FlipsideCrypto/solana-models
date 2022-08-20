{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH tx_counter AS (
    SELECT 
        value :block_id AS block_id, 
        count(*) AS tx_count
    FROM 
        {{ ref('bronze__block_txs_api') }}

    {% if is_incremental() %}
    WHERE
    _inserted_timestamp >= (
        SELECT
        MAX(_inserted_timestamp)
        FROM
        {{ this }}
    )
    {% endif %}
    GROUP BY 
        value :block_id
)

SELECT 
    value :block_id :: INTEGER AS block_id,
    to_timestamp(data :blockTime) AS block_timestamp, 
    'mainnet' AS tx_count, 
    'solana' AS chain_id, 
    tx_count, 
    data :blockHeight AS block_height, 
    TRIM(data :blockhash, '"') AS block_hash, 
    data :parentSlot AS previous_block_id, 
    TRIM(data :previousBlockhash, '"') AS previous_block_hash
FROM 
    {{ ref('bronze__blocks_api') }}
    b

LEFT OUTER JOIN tx_counter 
t 
ON b.block_id = t.block_id

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
{% endif %}