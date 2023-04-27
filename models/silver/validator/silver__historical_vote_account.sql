{{ config (
    materialized = 'view'
) }}

SELECT
    RIGHT(REGEXP_REPLACE(filename, '[^0-9]', ''), 3) AS epoch_ingested_at,
    json_data :account :data :parsed :info :authorizedVoters [0] :authorizedVoter :: STRING AS authorized_voter,
    json_data :account :data :parsed :info :authorizedVoters [0] :epoch :: STRING AS epoch,
    json_data :account :data :parsed :info :authorizedWithdrawer :: STRING AS authorized_withdrawer,
    json_data :account :data :parsed :info :commission :: NUMBER AS commission,
    json_data :account :data :parsed :info :epochCredits AS epoch_credits,
    json_data :account :data :parsed :info :lastTimestamp :slot :: NUMBER AS last_timestamp_slot,
    json_data :account :data :parsed :info :lastTimestamp :timestamp :: timestamp_tz AS last_timestamp,
    json_data :account :data :parsed :info :nodePubkey :: STRING AS node_pubkey,
    json_data :account :data :parsed :info :priorVoters AS prior_voters,
    json_data :account :data :parsed :info :rootSlot :: NUMBER AS root_slot,
    json_data :account :data :parsed :info :votes AS votes,
    json_data :account :lamports / pow(
        10,
        9
    ) AS account_sol,
    json_data :account :owner :: STRING AS owner,
    json_data :account :rentEpoch :: NUMBER AS rent_epoch,
    json_data :pubkey :: STRING AS vote_pubkey
FROM
    solana_dev.bronze.historical_vote_account_data
