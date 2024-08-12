{{ config(
  materialized = 'view',
  post_hook = 'ALTER VIEW {{this}} SET CHANGE_TRACKING = TRUE;',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'VALIDATOR' }} },
  tags = ['scheduled_non_core']
) }}

SELECT
  epoch_recorded :: INT AS epoch,
  stake_pubkey,
  vote_pubkey,
  authorized_staker,
  authorized_withdrawer,
  lockup,
  rent_exempt_reserve,
  credits_observed,
  activation_epoch,
  deactivation_epoch,
  active_stake,
  warmup_cooldown_rate,
  type_stake,
  program,
  account_sol,
  rent_epoch,
  COALESCE (
    snapshot_stake_accounts_id,
    {{ dbt_utils.generate_surrogate_key(
      ['epoch', 'stake_pubkey']
    ) }}
  ) AS fact_stake_accounts_id,
  COALESCE(
    inserted_timestamp,
    '2000-01-01'
  ) AS inserted_timestamp,
  COALESCE(
    modified_timestamp,
    '2000-01-01'
  ) AS modified_timestamp
FROM
  {{ ref('silver__snapshot_stake_accounts') }}
UNION ALL
SELECT
  epoch_ingested_at :: INT AS epoch,
  stake_pubkey,
  vote_pubkey,
  authorized_staker,
  authorized_withdrawer,
  lockup,
  rent_exempt_reserve,
  credits_observed,
  activation_epoch,
  deactivation_epoch,
  active_stake,
  warmup_cooldown_rate,
  type_stake,
  program,
  account_sol,
  rent_epoch,
  {{ dbt_utils.generate_surrogate_key(
    ['epoch', 'stake_pubkey']
  ) }} AS fact_stake_accounts_id,
  '2000-01-01' AS inserted_timestamp,
  '2000-01-01' AS modified_timestamp
FROM
  {{ ref('silver__historical_stake_account') }}
