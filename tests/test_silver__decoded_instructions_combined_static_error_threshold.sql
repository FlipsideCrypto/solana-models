WITH current_rates AS (
    SELECT
        program_id,
        block_timestamp :: DATE AS block_date,
        count_if(
            decoded_instruction :error :: STRING IS NOT NULL
        ) AS error_count,
        error_count / COUNT(*) AS error_rate
    FROM
        solana.silver.decoded_instructions_combined
    WHERE
        block_timestamp :: DATE = CURRENT_DATE - 1
    GROUP BY
        1,
        2
)
SELECT
    *
FROM
    current_rates
WHERE
    error_rate > 0.05;
