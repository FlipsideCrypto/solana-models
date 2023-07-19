{{ config (
    materialized = 'view'
) }}

SELECT
    DATE_FROM_PARTS(
        '20' || RIGHT(REGEXP_REPLACE(filename, '[^0-9]', ''), 2) :: INTEGER,
        SUBSTR(REGEXP_REPLACE(filename, '[^0-9]', ''), 1, 2) :: INTEGER,
        SUBSTR(REGEXP_REPLACE(filename, '[^0-9]', ''), 3, 2) :: INTEGER
    ) AS file_date,
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
    json_data :vote_account :: STRING as vote_pubkey,
    json_data :vote_distance_score :: NUMBER AS vote_distance_score,
    json_data :www_url :: STRING AS www_url
FROM
    {{ source(
        'bronze',
        'historical_validator_app_data'
    ) }}
