{{ config(
    materialized = 'incremental',
    unique_key = 'test_timestamp',
    full_refresh = false
) }}

WITH summary_stats AS (

    SELECT
        MIN(block_id) AS min_block,
        MAX(block_id) AS max_block,
        MIN(block_timestamp) AS min_block_timestamp,
        MAX(block_timestamp) AS max_block_timestamp,
        COUNT(1) AS blocks_tested
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_timestamp <= DATEADD('hour', -12, CURRENT_TIMESTAMP())

{% if is_incremental() %}
AND (
    block_id >= (
        SELECT
            MIN(block_id)
        FROM
            (
                SELECT
                    MIN(block_id) AS block_id
                FROM
                    {{ ref('silver__blocks') }}
                WHERE
                    block_timestamp BETWEEN DATEADD('hour', -96, CURRENT_TIMESTAMP())
                    AND DATEADD('hour', -95, CURRENT_TIMESTAMP())
                UNION
                SELECT
                    MIN(VALUE) - 1 AS block_id
                FROM
                    (
                        SELECT
                            blocks_impacted_array
                        FROM
                            {{ this }}
                            qualify ROW_NUMBER() over (
                                ORDER BY
                                    test_timestamp DESC
                            ) = 1
                    ),
                    LATERAL FLATTEN(
                        input => blocks_impacted_array
                    )
            )
    ) {% if var('OBSERV_FULL_TEST') %}
        OR block_id >= 0
    {% endif %}
)
{% endif %}
),
base_blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_id BETWEEN (
            SELECT
                min_block
            FROM
                summary_stats
        )
        AND (
            SELECT
                max_block
            FROM
                summary_stats
        )
),
actual_tx_counts AS (
    SELECT
        block_id,
        transaction_count
    FROM
        {{ ref('silver_observability__blocks_tx_count') }}
    WHERE
        block_id BETWEEN (
            SELECT
                min_block
            FROM
                summary_stats
        )
        AND (
            SELECT
                max_block
            FROM
                summary_stats
        )
),
expected_tx_counts AS (
    SELECT
        b.block_id,
        tc.transaction_count
    FROM
        base_blocks b
        LEFT OUTER JOIN {{ ref('silver___blocks_tx_count') }}
        tc
        ON tc.block_id = b.block_id
),
potential_missing_txs AS (
    SELECT
        e.block_id
    FROM
        expected_tx_counts e
        LEFT OUTER JOIN actual_tx_counts a
        ON e.block_id = a.block_id
    WHERE
        (
            (
                e.block_id < 226000000
                OR e.transaction_count IS NULL
            )
            AND a.block_id IS NULL
        )
        OR (
            e.block_id >= 226000000
            AND COALESCE(
                a.transaction_count,
                0
            ) <> e.transaction_count
        )
),
broken_blocks AS (
    SELECT
        m.block_id
    FROM
        potential_missing_txs m
        LEFT OUTER JOIN {{ ref('streamline__complete_block_txs') }}
        cmp
        ON m.block_id = cmp.block_id
    WHERE
        cmp.error IS NOT NULL
        OR cmp.block_id IS NULL
),
impacted_blocks AS (
    SELECT
        COUNT(1) AS blocks_impacted_count,
        ARRAY_AGG(block_id) within GROUP (
            ORDER BY
                block_id
        ) AS blocks_impacted_array
    FROM
        broken_blocks
)
SELECT
    'transactions' AS test_name,
    min_block,
    max_block,
    min_block_timestamp,
    max_block_timestamp,
    blocks_tested,
    blocks_impacted_count,
    blocks_impacted_array,
    SYSDATE() AS test_timestamp
FROM
    summary_stats
    JOIN impacted_blocks
    ON 1 = 1
