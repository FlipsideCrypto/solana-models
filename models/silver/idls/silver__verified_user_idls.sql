{{ config (
    materialized = "incremental",
    unique_key = "id",
    tags = ['idls']
) }}

WITH base AS (

    SELECT
        program_id,
        idl,
        SHA2(PARSE_JSON(idl)) AS idl_hash,
        discord_username,
        _inserted_timestamp
    FROM
        {{ source(
            "crosschain_public",
            "user_idls"
        ) }}
    WHERE
        blockchain = 'solana'
        AND NOT is_duplicate

{% if is_incremental() %}
AND program_id NOT IN (
    SELECT
        program_id
    FROM
        {{ this }}
)
AND _inserted_timestamp > (
    SELECT
        COALESCE(
            MAX(
                _inserted_timestamp
            ),
            '1970-01-01'
        )
    FROM
        {{ this }}
)
{% endif %}
ORDER BY
    _inserted_timestamp ASC
LIMIT
    10
), program_requests AS (
    SELECT
        e.program_id,
        ARRAY_CONSTRUCT(
            e.index,
            e.program_id,
            e.instruction,
            TRUE
        ) AS request
    FROM
        {{ ref('silver__events') }}
        e
        JOIN base b
        ON b.program_id = e.program_id
        AND succeeded
        AND block_timestamp >= CURRENT_DATE - 30 qualify(ROW_NUMBER() over (PARTITION BY e.program_id
    ORDER BY
        block_timestamp)) <= 100
),
groupings AS (
    SELECT
        program_id,
        ARRAY_AGG(request) AS requests
    FROM
        program_requests
    GROUP BY
        1
),
responses AS (
    SELECT
        program_id,
        streamline.udf_decode_instructions(requests) AS response
    FROM
        groupings
)
SELECT
    b.program_id,
    b.idl,
    b.idl_hash,
    b.discord_username,
    b._inserted_timestamp,
    CONCAT(
        b.program_id,
        '-',
        b.idl_hash
    ) AS id
FROM
    responses r
    JOIN base b
    ON b.program_id = r.program_id
WHERE
    r.response :status_code :: INTEGER = 200 qualify(ROW_NUMBER() over(PARTITION BY b.program_id
ORDER BY
    _inserted_timestamp DESC)) = 1