{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'VALIDATOR' }}},
    unique_key = ['fact_vote_accounts_id'],
    cluster_by = ['epoch','last_epoch_active'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(vote_pubkey, node_pubkey, owner)'),
    tags = ['scheduled_non_core'],
    full_refresh = false
) }}

{% if execute %}
    {% if is_incremental() %}
        {% set query %}
            SELECT MAX(modified_timestamp) AS max_modified_timestamp
            FROM {{ this }}
        {% endset %}

        {% set max_modified_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

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
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
-- historical data -- tables static and disabled, and manual change needed in rare case where fr is needed
{# 
{% if not is_incremental() %}
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
{% endif %}
#}
