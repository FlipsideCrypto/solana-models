WITH summary_stats AS (
    SELECT
        MIN(block_id) AS min_block,
        MAX(block_id) AS max_block,
        MIN(block_timestamp) AS min_block_timestamp,
        MAX(block_timestamp) AS max_block_timestamp,
        COUNT(1) AS blocks_tested
    FROM
        {{ ref('silver__blocks') }}
    WHERE block_timestamp <= DATEADD('hour', -12, CURRENT_TIMESTAMP())
        -- block_timestamp <= DATEADD('hour', -12, CURRENT_TIMESTAMP())
        -- AND block_timestamp >= DATEADD('day', -100, CURRENT_TIMESTAMP()) -- block_id between 192931291 and 192931296
        -- block_id between 192926963 and 192926968

    {% if is_incremental() %}
    AND (
        block_number >= (
            SELECT
                MIN(block_number)
            FROM
                (
                    SELECT
                        MIN(block_number) AS block_number
                    FROM
                        {{ ref('silver__blocks') }}
                    WHERE
                        block_timestamp BETWEEN DATEADD('hour', -96, CURRENT_TIMESTAMP())
                        AND DATEADD('hour', -95, CURRENT_TIMESTAMP())
                    UNION
                    SELECT
                        MIN(VALUE) - 1 AS block_number
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
            OR block_number >= 0
        {% endif %}
    )
    {% endif %})
,
base_blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_id >= 154195836 -- this query wont give correct results prior to this block_id
        AND block_id BETWEEN (
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
base_txs AS (
    SELECT
        block_id,
        tx_id
    FROM
        {{ ref('silver__votes') }}
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
    UNION
    SELECT
        block_id,
        tx_id
    FROM
        {{ ref('silver__transactions') }}
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
potential_missing_txs AS (
    SELECT
        base_blocks.*
    FROM
        base_blocks
        LEFT OUTER JOIN base_txs
        ON base_blocks.block_id = base_txs.block_id
    WHERE
        base_txs.block_id IS NULL
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
    CURRENT_TIMESTAMP() AS test_timestamp
FROM
    summary_stats
    JOIN impacted_blocks
    ON 1 = 1
