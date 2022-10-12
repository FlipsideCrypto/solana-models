WITH base_blocks AS (
    SELECT
        *
    FROM
        {% if target.database == 'SOLANA' %}
        solana.silver.blocks2
        {% else %}
        solana_dev.silver.blocks2
        {% endif %}
    WHERE
        block_id >= 154195836 -- this query wont give correct results prior to this block_id
        AND _inserted_date < CURRENT_DATE
),
base_txs AS (
    SELECT
        DISTINCT block_id
    FROM
        {% if target.database == 'SOLANA' %}
        solana.silver.transactions2
        {% else %}
        solana_dev.silver.transactions2
        {% endif %}
    WHERE
        block_id >= 154195836
    UNION
    SELECT
        DISTINCT block_id
    FROM
        {% if target.database == 'SOLANA' %}
        solana.silver.votes2
        {% else %}
        solana_dev.silver.votes2
        {% endif %}
    WHERE
        block_id >= 154195836
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
)
SELECT
    m.block_id
FROM
    potential_missing_txs m
    LEFT OUTER JOIN {{ ref('streamline__complete_block_txs') }} cmp
    ON m.block_id = cmp.block_id
WHERE
    cmp.error IS NOT NULL
    OR cmp.block_id IS NULL