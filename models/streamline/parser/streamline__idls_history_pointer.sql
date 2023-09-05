{{ config (
    materialized = 'table'
) }}

WITH base AS (

    SELECT
        SPLIT_PART(
            id,
            '-',
            3
        ) :: STRING AS program_id,
        MIN(block_id) AS min_decoded_block
    FROM
        {{ ref('streamline__complete_decoded_instructions') }}
    GROUP BY
        1
),
program_last_processed AS (
    SELECT
        b.*,
        bl.block_timestamp :: DATE AS min_decoded_block_timestamp_date
    FROM
        base b
        JOIN {{ ref('silver__blocks') }}
        bl
        ON bl.block_id = b.min_decoded_block
)
SELECT
    h.program_id,
    COALESCE(
        p.min_decoded_block_timestamp_date,
        h.default_backfill_start_block_timestamp
    ) :: DATE AS min_decoded_block_timestamp_date,
    COALESCE(
        p.min_decoded_block,
        h.default_backfill_start_block_id
    ) AS min_decoded_block_id,
    min_decoded_block_timestamp_date -2 AS backfill_to_date
FROM
    {{ ref('streamline__idls_history') }}
    h
    LEFT JOIN program_last_processed p
    ON p.program_id = h.program_id
WHERE 
    min_decoded_block_id > first_block_id
