{{ config(
    materialized = 'incremental',
    unique_key = ['profile_wallets_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(player_profile,wallet,profile_wallets_id)'
    ),
    tags = ['daily']
) }}

{% if execute %}

{% if is_incremental() %}
{% set max_modified_query %}

SELECT
    MAX(modified_timestamp) AS modified_timestamp
FROM
    {{ this }}

    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query) [0] [0] %}
{% endif %}
{% endif %}
SELECT
    CASE
        WHEN instruction :accounts [9] = 'CRAFT2RPXPJWCEix4WpJST3E7NLf79GTqZUL75wngXo5' --crafting
        THEN instruction :accounts [7]
        WHEN instruction :accounts [18] = 'DataJpxFgHhzwu4zYJeHCnAv21YqWtanEBphNxXBHdEY' --scanning
        THEN instruction :accounts [1]
        WHEN instruction :accounts [8] = 'CSTatsVpHbvZmwHbCjZKVfYQT5JXfsXccXufhEcwCqTg' --warp
        THEN instruction :accounts [1]
        WHEN instruction :accounts [4] = 'GAMEzqJehF8yAnKiTARUuhZMvLvkZVAsCVri5vSfemLr' --subwarp
        THEN instruction :accounts [1]
    END :: STRING AS player_profile,CASE
        WHEN instruction :accounts [9] = 'CRAFT2RPXPJWCEix4WpJST3E7NLf79GTqZUL75wngXo5' --crafting
        THEN instruction :accounts [6]
        WHEN instruction :accounts [18] = 'DataJpxFgHhzwu4zYJeHCnAv21YqWtanEBphNxXBHdEY' --scanning
        THEN instruction :accounts [0]
        WHEN instruction :accounts [8] = 'CSTatsVpHbvZmwHbCjZKVfYQT5JXfsXccXufhEcwCqTg' --warp
        THEN instruction :accounts [0]
        WHEN instruction :accounts [4] = 'GAMEzqJehF8yAnKiTARUuhZMvLvkZVAsCVri5vSfemLr' --subwarp
        THEN instruction :accounts [0]
    END :: STRING AS wallet,
    {{ dbt_utils.generate_surrogate_key(
        ['player_profile', 'wallet']
    ) }} AS profile_wallets_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__events') }}
WHERE
    program_id = 'SAGE2HAwep459SNq61LHvjxPk4pLPEJLoMETef7f7EE'
    AND (
        instruction :accounts [1] = player_profile
        OR instruction :accounts [7] = player_profile
    )
    AND (
        instruction :accounts [9] = 'CRAFT2RPXPJWCEix4WpJST3E7NLf79GTqZUL75wngXo5'
        OR instruction :accounts [18] = 'DataJpxFgHhzwu4zYJeHCnAv21YqWtanEBphNxXBHdEY'
        OR instruction :accounts [8] = 'CSTatsVpHbvZmwHbCjZKVfYQT5JXfsXccXufhEcwCqTg'
        OR instruction :accounts [4] = 'GAMEzqJehF8yAnKiTARUuhZMvLvkZVAsCVri5vSfemLr'
    )
    AND succeeded = 'true'
    AND wallet != 'FEEPAYye6DtaJhSXFR65ENXMroiseL1jqYT4QxHKRoZr'
    AND block_timestamp >= '2024-04-01'

{% if is_incremental() %}
AND modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
GROUP BY
    1,
    2
