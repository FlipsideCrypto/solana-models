WITH solscan_counts AS (
    SELECT
        s.*
    FROM
        solana.silver._blocks_tx_count s
        JOIN solana.silver.blocks b
        ON b.block_id = s.block_id
    WHERE
        b.block_timestamp :: DATE BETWEEN CURRENT_DATE - 8
        AND CURRENT_DATE - INTERVAL '12 HOUR'
),
silver_counts AS (
    SELECT
        block_id,
        SUM(transaction_count) AS transaction_count
    FROM
        (
            SELECT
                block_id,
                COUNT(block_id) AS transaction_count
            FROM
                {{ ref('silver__transactions') }}
                t
            WHERE
                block_timestamp :: DATE BETWEEN CURRENT_DATE - 8
                AND CURRENT_DATE - INTERVAL '12 HOUR'
            GROUP BY
                1
            UNION ALL
            SELECT
                block_id,
                COUNT(block_id) AS transaction_count
            FROM
                solana.silver.votes t
            WHERE
                block_timestamp :: DATE BETWEEN CURRENT_DATE - 8
                AND CURRENT_DATE - INTERVAL '12 HOUR'
            GROUP BY
                1
        )
    GROUP BY
        1
)
SELECT
    e.block_id,
    e.transaction_count AS ect,
    A.transaction_count AS act,
    e.transaction_count - A.transaction_count AS delta
FROM
    solscan_counts e
    LEFT OUTER JOIN silver_counts A
    ON e.block_id = A.block_id
WHERE
    ect <> 0
    AND (
        delta <> 0
        OR A.block_id IS NULL
    )