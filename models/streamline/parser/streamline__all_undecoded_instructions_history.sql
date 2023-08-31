{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_program_parser(object_construct('realtime', 'False'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH idls_in_play AS (

    SELECT
        *
    FROM
        {{ ref('streamline__idls_history') }}
    WHERE
        min_decoded_block <> first_event_block
        OR min_decoded_block IS NULL
),
load_range_existing AS (
    SELECT
        -- MAX(block_id) as max_block_completed,
        MIN(block_id) AS min_block_completed,
        min_block_completed - 500000 AS backfill_to_block,
        SPLIT_PART(
            id,
            '-',
            3
        ) :: STRING AS program_id
    FROM
        {{ ref('streamline__complete_decoded_instructions') }}
    GROUP BY
        program_id
),
new_idl AS (
    SELECT
        program_id
    FROM
        idls_in_play
    WHERE
        program_id NOT IN (
            SELECT
                DISTINCT(program_id)
            FROM
                load_range_existing
        )
),
load_range_new AS(
    SELECT
        MAX(block_id) AS latest_block,
        latest_block - 500000 AS backfill_to_block,
        program_id
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id IN (
            SELECT
                program_id
            FROM
                new_idl
        )
    GROUP BY
        program_id
),
block_ranges AS (
    SELECT
        latest_block AS start_block,
        backfill_to_block,
        program_id
    FROM
        load_range_new
    UNION ALL
    SELECT
        min_block_completed AS start_block,
        backfill_to_block,
        program_id
    FROM
        load_range_existing
)
SELECT
    e.program_id,
    e.tx_id,
    e.index,
    e.instruction,
    e.block_id,
    e.block_timestamp
FROM
    {{ ref('silver__events') }} e
    JOIN block_ranges b
    ON e.program_id = b.program_id
WHERE
    e.block_id BETWEEN backfill_to_block
    AND start_block
