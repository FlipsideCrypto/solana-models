{{ config(
    materialized = 'view',
    tags = ['streamline'],
) }}

WITH pre_final AS (

    SELECT
        SEQ8() + IFF(
            (
                SELECT
                    MAX(block_id) -1000000
                FROM
                    {{ ref('streamline__complete_block_txs') }}
            ) < 148693779,
            148693779,
            (
                SELECT
                    MAX(block_id) -1000000
                FROM
                    {{ ref('streamline__complete_block_txs') }}
            )
        ) AS block_id
    FROM
        TABLE(GENERATOR(rowcount => 5000000))
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__complete_block_txs') }}
),
max_block AS (
    SELECT
        MAX(block_id) AS max_block_id
    FROM
        {{ ref('silver__blocks') }}
),
base_blocks AS (
    SELECT
        *
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        block_id >= (
            SELECT
                max_block_id -2000000
            FROM
                max_block
        ) -- make assumption (since we have intraday alerts) that 2mil blocks prior to latest have been confirmed
        AND _inserted_date < CURRENT_DATE
),
base_txs AS (
    SELECT
        DISTINCT block_id
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_id >= (
            SELECT
                max_block_id -2000000
            FROM
                max_block
        )
    UNION
    SELECT
        DISTINCT block_id
    FROM
        {{ ref('silver__votes') }}
    WHERE
        block_id >= (
            SELECT
                max_block_id -2000000
            FROM
                max_block
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
solscan_discrepancy_retries AS (
    SELECT
        m.block_id
    FROM
        {{ source(
            'solana_test_silver',
            'transactions_and_votes_missing_7_days'
        ) }}
        m
        LEFT JOIN {{ ref('streamline__complete_block_txs') }} C
        ON C.block_id = m.block_id
    WHERE
        C._partition_id <= m._partition_id
    LIMIT 200
)
SELECT
    block_id,
    (
        SELECT
            COALESCE(MAX(_partition_id) + 1, 1)
        FROM
            {{ ref('streamline__complete_block_txs') }}
    ) AS batch_id
FROM
    pre_final
UNION
SELECT
    block_id,
    (
        SELECT
            COALESCE(MAX(_partition_id) + 1, 1)
        FROM
            {{ ref('streamline__complete_block_txs') }}
    ) AS batch_id
FROM
    solscan_discrepancy_retries
UNION
SELECT
    m.block_id,
    (
        SELECT
            COALESCE(MAX(_partition_id) + 1, 1)
        FROM
            {{ ref('streamline__complete_block_txs') }}
    ) AS batch_id
FROM
    potential_missing_txs m
    LEFT OUTER JOIN {{ ref('streamline__complete_block_txs') }}
    cmp
    ON m.block_id = cmp.block_id
WHERE
    cmp.error IS NOT NULL
    OR cmp.block_id IS NULL
