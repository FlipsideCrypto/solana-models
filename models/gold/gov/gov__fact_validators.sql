{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'VALIDATOR' }}},
  tags = ['scheduled_non_core']
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
  www_url
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
  www_url
FROM
  {{ ref('silver__historical_validator_app_data') }}
