{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', epoch_recorded, vote_pubkey)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
) }}

WITH base AS (

  SELECT
    json_data :status :: STRING AS epoch_status,
    json_data :data :activatedStake :: int / pow(
        10,
        9
    ) AS activatedStake,
    json_data :data :commission :: NUMBER AS commission,
    json_data :data :epochCredits [4] [0] :: NUMBER AS latest_epoch,
    json_data :data :epochCredits AS epochCredits,
    json_data :data :epochVoteAccount :: BOOLEAN AS epochVoteAccount,
    json_data :data :lastVote :: NUMBER AS lastVote,
    json_data :data :nodePubkey :: STRING AS nodePubkey,
    json_data :data :rootSlot :: NUMBER AS rootSlot,
    json_data :data :votePubkey :: STRING AS votePubkey,
    _inserted_timestamp
  FROM
    {{ ref('bronze__vote_accounts') }}

{% if is_incremental() %}
WHERE _inserted_timestamp > (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
votes_accounts_epoch_recorded AS (
  SELECT
    A.*,
    b.epoch_recorded
  FROM
    base A
    LEFT JOIN (
      SELECT
        MAX(latest_epoch) AS epoch_recorded,
        _inserted_timestamp
      FROM
        base
      GROUP BY
        _inserted_timestamp
    ) b
    ON A._inserted_timestamp = b._inserted_timestamp
),
votes_accounts_deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY epoch_recorded, votePubkey ORDER BY _inserted_timestamp desc) AS row_num
  FROM
    votes_accounts_epoch_recorded
)
SELECT
  epoch_status,
  epoch_recorded,
  activatedStake / pow(
        10,
        9
    ) AS active_stake,
  commission,
  epochCredits AS epoch_credits,
  epochVoteAccount AS epoch_vote_account,
  lastVote AS last_vote,
  nodePubkey AS node_pubkey,
  rootSlot AS root_slot,
  votePubkey AS vote_pubkey,
  _inserted_timestamp
FROM
  votes_accounts_deduped
  where row_num = 1
