WITH current_rates AS (
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
    *
FROM
    current_rates
WHERE
    error_rate > 0.01
