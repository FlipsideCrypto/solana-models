{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'VALIDATOR' }} },
    unique_key = ['fact_block_production_id'],
    cluster_by = ['epoch'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(node_pubkey)'),
    tags = ['scheduled_non_core'],
    full_refresh = false
) }}

{% if execute %}
    {% if is_incremental() %}
        {% set query %}
            SELECT 
              max(modified_timestamp) AS max_modified_timestamp
            FROM 
              {{ this }}
        {% endset %}
        {% set max_modified_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

WITH base_block_production AS (
  SELECT
    epoch,
    node_pubkey,
    sum(num_leader_slots) AS num_leader_slots,
    sum(num_blocks_produced) AS num_blocks_produced,
    max(inserted_timestamp) AS inserted_timestamp,
    max(modified_timestamp) AS modified_timestamp
  FROM
    {{ ref('silver__snapshot_block_production_2') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
  GROUP BY 1,2
)
SELECT
  bp.epoch,
  node_pubkey,
  num_leader_slots,
  num_blocks_produced,
  e.start_block AS start_slot,
  e.end_block AS end_slot,
  {{ dbt_utils.generate_surrogate_key(
    ['bp.epoch', 'node_pubkey']
  ) }} AS fact_block_production_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp
FROM
  base_block_production AS bp
LEFT JOIN
  {{ ref('silver__epoch') }} AS e
  ON bp.epoch = e.epoch
  -- historical data -- tables static and disabled, and manual change needed in rare case where fr is needed
{# 
{% if not is_incremental() %}
UNION ALL
SELECT
  epoch::int as epoch,
  node_pubkey,
  num_leader_slots,
  num_blocks_produced,
  start_slot,
  end_slot,
  {{ dbt_utils.generate_surrogate_key(
    ['epoch', 'node_pubkey']
  ) }} AS fact_block_production_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp
FROM
  {{ ref('silver__historical_block_production') }}
{% endif %}
#}
