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
min_decoded_event AS (
    SELECT
        SPLIT_PART(ID, '-', 3)::string as program_id,
        MIN(block_id) AS min_decoded_block
    FROM
        {{ ref('streamline__complete_decoded_instructions') }}
    WHERE
        LOWER(program_id) IN (
            SELECT
                program_id
            FROM
                idls
        )
    GROUP BY
        program_id
),
event_history AS (
    SELECT
        program_id,
        MIN(block_id) AS first_event_block,
        MAX(block_id) AS latest_event_block
    FROM
        solana.silver.events
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
    A.program_id,
    A.first_event_block,
    A.latest_event_block,
    b.min_decoded_block
FROM
    event_history A
    LEFT JOIN min_decoded_event b
    ON A.program_id = b.program_id
