{{ config (
    materialized = 'table'
) }}

WITH idls AS (

    SELECT
        LOWER(
            REPLACE(SPLIT_PART(metadata$filename, '/', 3), '.json')
        ) AS program_id
    FROM
        {{ source(
            'bronze_streamline',
            'decode_instructions_idls'
        ) }}
)

,
event_history AS (
    SELECT
        program_id,
        MIN(block_timestamp) AS first_event_block_timestamp,
        MAX(block_timestamp) AS latest_event_block_timestamp
    FROM
        {{ ref('silver__events') }}
    WHERE
        LOWER(program_id) IN (
            SELECT
                program_id
            FROM
                idls
        )
    GROUP BY
        program_id
)

SELECT
    program_id,
    first_event_block_timestamp,
    latest_event_block_timestamp
FROM
    event_history