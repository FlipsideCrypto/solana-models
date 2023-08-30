{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_program_parser(object_construct('realtime', 'False'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH min_decoded_event AS (

    SELECT
        SPLIT_PART(
            id,
            '-',
            3
        ) :: STRING AS program_id,
        MIN(block_id) AS min_decoded_block
    FROM
        {{ ref('streamline__complete_decoded_instructions') }} A
    GROUP BY
        program_id
),
min_decoded_event_timestamp AS (
    SELECT
        A.*,
        b.block_timestamp AS min_decoded_block_timestamp
    FROM
        min_decoded_event A
        JOIN {{ ref('silver__blocks') }}
        b
        ON A.min_decoded_block = b.block_id
),
idls_in_play AS (
    SELECT
        A.*,
        b.min_decoded_block_timestamp
    FROM
        {{ ref('streamline__idls_history') }} A
        LEFT JOIN min_decoded_event_timestamp b
        ON A.program_id = b.program_id
    WHERE
        first_event_block_timestamp <> min_decoded_block_timestamp
        OR min_decoded_block_timestamp IS NULL
),
block_timestamp_ranges AS (
    SELECT
        CASE
            WHEN min_decoded_block_timestamp IS NOT NULL THEN min_decoded_block_timestamp
            ELSE latest_event_block_timestamp
        END AS start_range,
        CASE
            WHEN min_decoded_block_timestamp IS NOT NULL THEN DATEADD(
                DAY,
                -2,
                min_decoded_block_timestamp
            )
            ELSE DATEADD(
                DAY,
                -2,
                latest_event_block_timestamp
            )
        END AS end_range,
        program_id
    FROM
        idls_in_play
)
SELECT
    e.program_id,
    e.tx_id,
    e.index,
    e.instruction,
    e.block_id,
    e.block_timestamp
FROM
    {{ ref('silver__events') }}
    e
    JOIN block_timestamp_ranges b
    ON e.program_id = b.program_id
WHERE
    e.block_timestamp BETWEEN end_range
    AND start_range
