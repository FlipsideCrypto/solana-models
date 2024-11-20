{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

WITH pre_final AS (
  SELECT 
    block_id, 
    max(block_timestamp) as block_timestamp,
    count(block_id) AS num_votes
  FROM {{ ref('silver__votes') }}
  
  {% if is_incremental() %}
     WHERE _inserted_timestamp::date >= current_date - 1
  {% endif %}

  GROUP BY block_id
)

SELECT 
  block_timestamp,
  block_id, 
  num_votes,
  {{ dbt_utils.generate_surrogate_key(
        ['block_id']
  ) }} AS votes_agg_block_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM pre_final