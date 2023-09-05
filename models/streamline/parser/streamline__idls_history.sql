{{ config (
    materialized = "incremental",
    unique_key = 'program_id'
) }}

WITH idls AS (

    SELECT
        program_id
    FROM
        {{ ref('silver__verified_idls') }}

{% if is_incremental() %}
WHERE
    program_id NOT IN (
        SELECT
            program_id
        FROM
            {{ this }}
    )
{% endif %}
),
event_history AS (
    SELECT
        program_id,
        MIN(block_id) AS first_block_id,
        MAX(block_timestamp) AS default_backfill_start_block_timestamp,
        MAX(block_id) AS default_backfill_start_block_id
    FROM
        {{ ref('silver__events') }}
    WHERE
        program_id IN (
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
    first_block_id,
    default_backfill_start_block_timestamp,
    default_backfill_start_block_id
FROM
    event_history
