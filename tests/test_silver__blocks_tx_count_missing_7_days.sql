SELECT
    b.block_id
FROM
    solana.silver.blocks b
    LEFT OUTER JOIN {{ ref('silver___blocks_tx_count') }}
    b2
    ON b.block_id = b2.block_id
WHERE
    b.block_id >= 226000000
    AND b.block_timestamp BETWEEN CURRENT_DATE - 8
    AND CURRENT_TIMESTAMP - INTERVAL '12 HOUR'
    AND b2.block_id IS NULL
