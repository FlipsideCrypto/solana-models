{{
    config(
        tags=["test_weekly"]
    )
}}

WITH base AS (
    SELECT
        program_id,
        block_timestamp :: DATE AS block_date,
        count_if(
            decoded_instruction :error :: STRING IS NOT NULL
        ) AS daily_error_count,
        COUNT(*) AS daily_count
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        block_timestamp BETWEEN CURRENT_DATE - 92
        AND CURRENT_DATE - 2
    GROUP BY
        1,
        2
),
daily_error_rates AS (
    SELECT
        program_id,
        block_date,
        daily_error_count / daily_count AS daily_error_rate
    FROM
        base
),
past_rates AS (
    SELECT
        program_id,
        AVG(daily_error_rate) AS avg_daily_error_rate_90_day
    FROM
        daily_error_rates
    GROUP BY
        1
),
current_rates AS (
    SELECT
        program_id,
        block_timestamp :: DATE AS block_date,
        count_if(
            decoded_instruction :error :: STRING IS NOT NULL
        ) AS error_count,
        error_count / COUNT(*) AS error_rate
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        block_timestamp :: DATE >= CURRENT_DATE - 7
    GROUP BY
        1,
        2
)
SELECT
    C.*
FROM
    current_rates C
    JOIN past_rates p
    ON p.program_id = C.program_id
WHERE
    C.error_rate > p.avg_daily_error_rate_90_day
