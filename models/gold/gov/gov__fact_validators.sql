{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'VALIDATOR' }} },
  tags = ['scheduled_non_core','exclude_change_tracking']
) }}

SELECT
  epoch_recorded :: INT AS epoch,
  node_pubkey,
  vote_pubkey,
  active_stake,
  admin_warning,
  avatar_url,
  commission,
  created_at,
  data_center_host,
  data_center_key,
  delinquent,
  details,
  epoch_active,
  epoch_credits,
  keybase_id,
  latitude,
  longitude,
  validator_name,
  software_version,
  updated_at,
  www_url,
  COALESCE (
    snapshot_validators_app_data_id,
    {{ dbt_utils.generate_surrogate_key(
      ['epoch', 'node_pubkey']
    ) }}
  ) AS fact_validators_id,
  COALESCE(
    inserted_timestamp,
    '2000-01-01'
  ) AS inserted_timestamp,
  COALESCE(
    modified_timestamp,
    '2000-01-01'
  ) AS modified_timestamp
FROM
  {{ ref('silver__snapshot_validators_app_data') }}
UNION ALL
SELECT
  epoch_recorded :: INT AS epoch,
  node_pubkey,
  vote_pubkey,
  active_stake,
  admin_warning,
  avatar_url,
  commission,
  created_at,
  data_center_host,
  data_center_key,
  delinquent,
  details,
  epoch_active,
  epoch_credits,
  keybase_id,
  latitude,
  longitude,
  validator_name,
  software_version,
  updated_at,
  www_url,
  {{ dbt_utils.generate_surrogate_key(
    ['epoch', 'node_pubkey']
  ) }} AS fact_validators_id,
  '2000-01-01' AS inserted_timestamp,
  '2000-01-01' AS modified_timestamp
FROM
  {{ ref('silver__historical_validator_app_data') }}
