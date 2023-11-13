{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', epoch_recorded, vote_pubkey)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
  tags = ['validator']
) }}

WITH base AS (

  SELECT
    _inserted_timestamp,
    json_data :account :data :parsed :info :authorizedVoters [0] :authorizedVoter :: STRING AS authorized_voter,
    json_data :account :data :parsed :info :authorizedVoters [0] :epoch :: NUMBER AS last_epoch_active,
    json_data :account :data :parsed :info :authorizedWithdrawer :: STRING AS authorized_withdrawer,
    json_data :account :data :parsed :info :commission :: NUMBER AS commission,
    json_data :account :data :parsed :info :epochCredits :: ARRAY AS epoch_credits,
    json_data :account :data :parsed :info :lastTimestamp :slot :: NUMBER AS last_timestamp_slot,
    json_data :account :data :parsed :info :lastTimestamp :timestamp :: timestamp_tz AS last_timestamp,
    json_data :account :data :parsed :info :nodePubkey :: STRING AS node_pubkey,
    json_data :account :data :parsed :info :priorVoters :: ARRAY AS prior_voters,
    json_data :account :data :parsed :info :rootSlot :: NUMBER AS root_slot,
    json_data :account :data :parsed :info :votes :: ARRAY AS votes,
    json_data :account :lamports / pow(
        10,
        9
    ) AS account_sol,
    json_data :account :owner :: STRING AS owner,
    json_data :account :rentEpoch :: NUMBER AS rent_epoch,
    json_data :pubkey :: STRING AS vote_pubkey
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
vote_accounts_epoch_recorded AS (
  SELECT
    a.*,
    b.epoch_recorded
  FROM
    base a
    LEFT JOIN (
      SELECT
        MAX(last_epoch_active) AS epoch_recorded,
        _inserted_timestamp
      FROM
        base
      GROUP BY
        _inserted_timestamp
    ) b
    ON A._inserted_timestamp = b._inserted_timestamp
)

select * from vote_accounts_epoch_recorded
qualify(ROW_NUMBER() over(PARTITION BY epoch_recorded, vote_pubkey
ORDER BY
    _inserted_timestamp DESC)) = 1