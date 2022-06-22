{{ config(
  materialized = 'incremental',
  unique_key = "block_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH base_tables AS (

  SELECT
    *
  FROM
    {{ ref('bronze__blocks') }}

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp)
    FROM
      {{ this }}
  )
{% endif %}
)
SELECT
  block_id,
  block_timestamp,
  network,
  chain_id,
  tx_count,
  header :blockHeight :: INTEGER AS block_height,
  header :blockhash :: VARCHAR AS block_hash,
  header :parentSlot :: INTEGER AS previous_block_id,
  header :previousBlockhash :: VARCHAR AS previous_block_hash,
  ingested_at,
  _inserted_timestamp
FROM
  base_tables qualify(ROW_NUMBER() over(PARTITION BY block_id
ORDER BY
  ingested_at DESC)) = 1
