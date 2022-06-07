{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH v AS (
  SELECT 
    block_id, 
    count(block_id) AS num_votes
  FROM {{ ref('silver__votes') }}
  
  {% if is_incremental() %}
     WHERE ingested_at :: DATE >= CURRENT_DATE - 5
  {% endif %}

  GROUP BY block_id
)

SELECT 
  t.block_timestamp,
  v.block_id, 
  num_votes

FROM v

INNER JOIN (SELECT DISTINCT block_id, block_timestamp FROM {{ ref('silver__transactions') }}) t 
ON v.block_id = t.block_id
