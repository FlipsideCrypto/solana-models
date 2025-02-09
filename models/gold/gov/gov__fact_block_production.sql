{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'VALIDATOR' }} },
  tags = ['scheduled_non_core','exclude_change_tracking']
) }}

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
  COALESCE(bp.inserted_timestamp,
    '2000-01-01'
  ) AS inserted_timestamp,
  COALESCE(
    bp.modified_timestamp,
    '2000-01-01'
  ) AS modified_timestamp
FROM
  base_block_production AS bp
LEFT JOIN
  {{ ref('silver__epoch') }} AS e
  ON bp.epoch = e.epoch
UNION ALL
SELECT
  epoch,
  node_pubkey,
  num_leader_slots,
  num_blocks_produced,
  start_slot,
  end_slot,
  {{ dbt_utils.generate_surrogate_key(
    ['epoch', 'node_pubkey']
  ) }} AS fact_block_production_id,
  '2000-01-01' AS inserted_timestamp,
  '2000-01-01' AS modified_timestamp
FROM
  {{ ref('silver__historical_block_production') }}
