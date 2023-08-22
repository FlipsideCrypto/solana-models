{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'VALIDATOR' }}}
) }}

SELECT
  epoch,
  node_pubkey,
  num_leader_slots,
  num_blocks_produced,
  start_slot,
  end_slot
FROM
  {{ ref('silver__snapshot_block_production') }}
UNION ALL
SELECT
  epoch,
  node_pubkey,
  num_leader_slots,
  num_blocks_produced,
  start_slot,
  end_slot
FROM
  {{ ref('silver__historical_block_production') }}
