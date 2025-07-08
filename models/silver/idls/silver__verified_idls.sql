{{ config(
    materialized = 'incremental',
    unique_key = "program_id",
    tags = ['idls']
) }}

WITH new_user_idls AS (
    SELECT
        program_id,
        idl,
        discord_username,
        _inserted_timestamp,
        'user' AS idl_source,
        idl_hash,
        TRUE as is_new_record
    FROM
        {{ ref('silver__verified_user_idls') }}
    WHERE 
        is_valid
{% if is_incremental() %}
    AND
        _inserted_timestamp > (
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
{% endif %}
),
all_idls AS (
    {% if is_incremental() %}
    -- Include existing records
    SELECT 
        program_id,
        idl,
        discord_username,
        _inserted_timestamp,
        idl_source,
        idl_hash,
        CASE
            WHEN _inserted_timestamp >= CURRENT_TIMESTAMP - INTERVAL '2 days' THEN TRUE
            ELSE FALSE
        END as is_new_record
    FROM {{ this }}
    WHERE program_id not in (select program_id from new_user_idls)
    
    UNION ALL
    {% endif %}
    
    -- Include new records
    SELECT 
        program_id,
        idl,
        discord_username,
        _inserted_timestamp,
        idl_source,
        idl_hash,
        is_new_record
    FROM new_user_idls
),
recent_activity AS (
    SELECT 
        program_id,
        MAX(_inserted_timestamp) as last_activity_ts,
        COUNT(*) as activity_count
    FROM 
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE 
        _inserted_timestamp >= CURRENT_DATE - 14
    GROUP BY 
        program_id
)
SELECT
    a.program_id,
    a.idl,
    a._inserted_timestamp,
    a.idl_source,
    a.discord_username,
    a.idl_hash,
    CASE 
        WHEN a.is_new_record THEN TRUE  -- Records from last 2 days are always active
        WHEN r.program_id IS NOT NULL AND r.activity_count > 0 THEN TRUE  -- Existing records with recent activity
        ELSE FALSE  -- No recent activity
    END as is_active,
    COALESCE(r.last_activity_ts, a._inserted_timestamp) as last_activity_timestamp
FROM
    all_idls a
LEFT JOIN 
    recent_activity r
    ON a.program_id = r.program_id