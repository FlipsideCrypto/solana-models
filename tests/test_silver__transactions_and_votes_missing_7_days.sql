{{
    config(
        tags=["test_hourly"]
    )
}}

WITH solscan_counts AS (
    SELECT
        s.*
    FROM
        solana.silver._blocks_tx_count s
    JOIN 
        solana.silver.blocks b
        ON b.block_id = s.block_id
    WHERE
        b.block_timestamp BETWEEN current_date - 8 AND current_timestamp - INTERVAL '12 HOUR'
),
silver_counts AS (
    SELECT
        block_id,
        count(tx_id) AS transaction_count
    FROM
        (
            SELECT
                block_id,
                tx_id
            FROM
                {{ ref('silver__transactions') }} t
            WHERE
                block_timestamp BETWEEN current_date - 8 AND current_timestamp - INTERVAL '12 HOUR'
            UNION
            SELECT
                block_id,
                tx_id
            FROM
                solana.silver.votes t
            WHERE
                block_timestamp BETWEEN current_date - 8 AND current_timestamp - INTERVAL '12 HOUR'
        )
    GROUP BY
        1
)
SELECT
    e.block_id,
    e.transaction_count AS ect,
    A.transaction_count AS act,
    e.transaction_count - A.transaction_count AS delta,
    coalesce(c._partition_id, c2._partition_id, 0) AS _partition_id
FROM
    solscan_counts e
LEFT OUTER JOIN 
    silver_counts A
    ON e.block_id = A.block_id
LEFT OUTER JOIN 
    streamline.complete_block_txs c
    ON e.block_id = c.block_id
LEFT OUTER JOIN 
    streamline.complete_block_txs_2 c2
    ON e.block_id = c2.block_id
WHERE
    ect <> 0
    AND (
        delta <> 0
        OR A.block_id IS NULL
    )