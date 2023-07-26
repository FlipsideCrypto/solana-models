{{ config(
      materialized='view'
    ) 
}}

SELECT
  epoch_recorded::INT as epoch,
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
  rent_epoch
FROM {{ ref('silver__snapshot_stake_accounts') }}
UNION ALL
SELECT
  epoch_ingested_at::INT as epoch,
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
  rent_epoch
FROM {{ ref('silver__historical_stake_account') }}