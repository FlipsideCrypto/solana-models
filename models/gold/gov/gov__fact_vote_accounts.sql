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
  rent_epoch,
  COALESCE (
        snapshot_vote_accounts_id,
        {{ dbt_utils.generate_surrogate_key(
            ['epoch', 'vote_pubkey']
        ) }}
    ) AS fact_vote_accounts_id,
  COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
  COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
  rent_epoch,
  {{ dbt_utils.generate_surrogate_key(
        ['epoch', 'vote_pubkey']
  ) }} AS fact_vote_accounts_id,
  '2000-01-01' as inserted_timestamp,
  '2000-01-01' AS modified_timestamp
FROM
  {{ ref('silver__historical_vote_account') }}
