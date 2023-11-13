{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'VALIDATOR' }}},
  tags = ['scheduled_non_core']
) }}

SELECT
  epoch_recorded :: INT AS epoch,
  vote_pubkey,
  node_pubkey,
  authorized_voter,
  authorized_withdrawer,
  commission,
  epoch_credits,
  last_epoch_active,
  last_timestamp_slot,
  last_timestamp,
  prior_voters,
  root_slot,
  votes,
  account_sol,
  owner,
  rent_epoch
FROM
  {{ ref('silver__snapshot_vote_accounts') }}
UNION ALL
SELECT
  epoch_ingested_at :: INT AS epoch,
  vote_pubkey,
  node_pubkey,
  authorized_voter,
  authorized_withdrawer,
  commission,
  epoch_credits,
  epoch :: INT AS last_epoch_active,
  last_timestamp_slot,
  last_timestamp,
  prior_voters,
  root_slot,
  votes,
  account_sol,
  owner,
  rent_epoch
FROM
  {{ ref('silver__historical_vote_account') }}
