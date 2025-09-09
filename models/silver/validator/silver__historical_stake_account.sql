{{ config (
    materialized = 'table',
    tags = ['validator_historical'],
    full_refresh = false,
    enabled = false
) }}

SELECT
    RIGHT(REGEXP_REPLACE(filename, '[^0-9]', ''), 3) AS epoch_ingested_at,
    json_data :account :data :parsed :info :meta :authorized :staker :: STRING AS authorized_staker,
    json_data :account :data :parsed :info :meta :authorized :withdrawer :: STRING AS authorized_withdrawer,
    json_data :account :data :parsed :info :meta :lockup :: OBJECT AS lockup,
    json_data :account :data :parsed :info :meta :rentExemptReserve :: NUMBER AS rent_exempt_reserve,
    json_data :account :data :parsed :info :stake :creditsObserved :: NUMBER AS credits_observed,
    json_data :account :data :parsed :info :stake :delegation :activationEpoch :: NUMBER AS activation_epoch,
    json_data :account :data :parsed :info :stake :delegation :deactivationEpoch :: NUMBER AS deactivation_epoch,
    json_data :account :data :parsed :info :stake :delegation :stake :: NUMBER / pow(
        10,
        9
    ) AS active_stake,
    json_data :account :data :parsed :info :stake :delegation :voter :: STRING AS vote_pubkey,
    json_data :account :data :parsed :info :stake :delegation :warmupCooldownRate :: NUMBER AS warmup_cooldown_rate,
    json_data :account :data :parsed :type :: STRING AS type_stake,
    json_data :account :data :program :: STRING AS program,
    json_data :account :lamports / pow(
        10,
        9
    ) AS account_sol,
    json_data :account :rentEpoch :: NUMBER AS rent_epoch,
    json_data :pubkey :: STRING AS stake_pubkey
FROM
    {{ source(
        'bronze',
        'historical_stake_account_data'
    ) }}
