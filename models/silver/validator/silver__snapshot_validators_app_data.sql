{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', epoch_recorded, node_pubkey)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['_inserted_timestamp::DATE'],
    tags = ['validator']
) }}

WITH base AS (

    SELECT
        json_data :account :: STRING AS node_pubkey,
        json_data :active_stake :: NUMBER AS active_stake,
        json_data :admin_warning :: STRING AS admin_warning,
        json_data :authorized_withdrawer_score :: STRING AS authorized_withdrawer_score,
        json_data :avatar_url :: STRING AS avatar_url,
        json_data :commission :: NUMBER AS commission,
        json_data :consensus_mods_score :: NUMBER AS consensus_mods_score,
        json_data :created_at :: STRING AS created_at,
        json_data :data_center_concentration_score :: STRING AS data_center_concentration_score,
        json_data :data_center_host :: STRING AS data_center_host,
        json_data :data_center_key :: STRING AS data_center_key,
        json_data :delinquent :: BOOLEAN AS delinquent,
        json_data :details :: STRING AS details,
        json_data :epoch :: NUMBER AS epoch_active,
        json_data :epoch_credits :: NUMBER AS epoch_credits,
        json_data :keybase_id :: STRING AS keybase_id,
        json_data :latitude :: STRING AS latitude,
        json_data :longitude :: STRING AS longitude,
        json_data :name :: STRING AS validator_name,
        json_data :published_information_score :: NUMBER AS published_information_score,
        json_data :root_distance_score :: NUMBER AS root_distance_score,
        json_data :security_report_score :: NUMBER AS security_report_score,
        json_data :skipped_slot_score :: NUMBER AS skipped_slot_score,
        json_data :skipped_slot :: NUMBER AS skipped_slot,
        json_data :skipped_slot_percent :: NUMBER AS skipped_slot_percent,
        json_data :software_version :: STRING AS software_version,
        json_data :software_version_score :: NUMBER AS software_version_score,
        json_data :stake_concentration_score :: NUMBER AS stake_concentration_score,
        json_data :total_score :: NUMBER AS total_score,
        json_data :updated_at :: STRING AS updated_at,
        json_data :vote_account :: STRING AS vote_pubkey,
        json_data :vote_distance_score :: NUMBER AS vote_distance_score,
        json_data :www_url :: STRING AS www_url,
        _inserted_timestamp
    FROM
        {{ ref('bronze__validators_app_api') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp > (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
validators_epoch_recorded AS (
    SELECT
        A.*,
        b.epoch_recorded
    FROM
        base A
        LEFT JOIN (
            SELECT
                MAX(epoch_active) AS epoch_recorded,
                _inserted_timestamp
            FROM
                base
            WHERE
                delinquent = FALSE
                AND active_stake > 0
            GROUP BY
                _inserted_timestamp
        ) b
        ON A._inserted_timestamp = b._inserted_timestamp
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['epoch_recorded', 'node_pubkey']
    ) }} AS snapshot_validators_app_data_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    validators_epoch_recorded qualify(ROW_NUMBER() over(PARTITION BY epoch_recorded, node_pubkey
ORDER BY
    _inserted_timestamp DESC)) = 1
