{{ config(
    materialized = 'incremental',
    unique_key = "program_id",
    tags = ['idls']
) }}

WITH user_abis AS (
    SELECT
        program_id,
        idl,
        discord_username,
        _inserted_timestamp,
        'user' AS idl_source,
        idl_hash
    FROM
        {{ ref('silver__verified_user_idls') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            COALESCE(
                MAX(
                    _inserted_timestamp
                ),
                '1970-01-01'
            )
        FROM
            {{ this }}
        WHERE
            idl_source = 'user'
    )
    AND program_id NOT IN (
        SELECT
            program_id
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    program_id,
    idl,
    _inserted_timestamp,
    idl_source,
    discord_username,
    idl_hash
FROM
    user_abis