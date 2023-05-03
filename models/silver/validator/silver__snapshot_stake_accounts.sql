{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', epoch_recorded, stake_pubkey)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_inserted_timestamp::DATE'],
) }}

WITH base AS (
SELECT
    TO_TIMESTAMP(
    CONCAT(
      '20', -- Adding the century prefix to convert 'yymmdd' to 'yyyymmdd'
      REGEXP_SUBSTR(filename, '\\/([0-9]{6})_', 1, 1, 'e') -- Extracting the 'yymmdd' part using REGEXP_SUBSTR function
    ),
    'YYYYMMDD'
  ) AS _inserted_timestamp,
    data :account :data :parsed :info :meta :authorized :staker :: STRING AS authorized_staker,
    data :account :data :parsed :info :meta :authorized :withdrawer :: STRING AS authorized_withdrawer,
    data :account :data :parsed :info :meta :lockup :: variant AS lockup,
    data :account :data :parsed :info :meta :rentExemptReserve :: NUMBER AS rent_exempt_reserve,
    data :account :data :parsed :info :stake :creditsObserved :: NUMBER AS credits_observed,
    data :account :data :parsed :info :stake :delegation :activationEpoch :: NUMBER AS activation_epoch,
    data :account :data :parsed :info :stake :delegation :deactivationEpoch :: NUMBER AS deactivation_epoch,
    data :account :data :parsed :info :stake :delegation :stake :: NUMBER / pow(
        10,
        9
    ) AS active_stake,
    data :account :data :parsed :info :stake :delegation :voter :: STRING AS vote_pubkey,
    data :account :data :parsed :info :stake :delegation :warmupCooldownRate :: NUMBER AS warmup_cooldown_rate,
    data :account :data :parsed :type :: STRING AS type_stake,
    data :account :data :program :: STRING AS program,
    data :account :lamports / pow(
        10,
        9
    ) AS account_sol,
    data :account :rentEpoch :: NUMBER AS rent_epoch,
    data :pubkey :: STRING AS stake_pubkey
  FROM
    solana_dev.bronze.temp_streamline_stake_program_accounts

{% if is_incremental() %}
WHERE _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp)
  FROM
    {{ this }}
)
{% endif %}
),
stake_accounts_epoch_recorded AS (
  SELECT
    A.*,
    b.epoch_recorded
  FROM
    base A
    LEFT JOIN (
      SELECT
        MAX(activation_epoch) AS epoch_recorded,
        _inserted_timestamp
      FROM
        base
      GROUP BY
        _inserted_timestamp
    ) b
    ON A._inserted_timestamp = b._inserted_timestamp
)

Select * from stake_accounts_epoch_recorded
