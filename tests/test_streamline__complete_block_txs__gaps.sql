/* TODO: Update this to be the block id of the cutover before merge */
{% set cutover_block_id = 307000000 %}

WITH base_blocks AS (
    SELECT
        *
    FROM
        {% if target.database == 'SOLANA' %}
        solana.silver.blocks
        {% else %}
        solana_dev.silver.blocks
        {% endif %}
    WHERE
        block_id >= 306000000 -- this query wont give correct results prior to this block_id
        AND _inserted_date < CURRENT_DATE
),
base_txs AS (
    SELECT
        DISTINCT block_id
    FROM
        {% if target.database == 'SOLANA' %}
        solana.silver.transactions
        {% else %}
        solana_dev.silver.transactions
        {% endif %}
    WHERE
        block_id >= 306000000
    UNION
    SELECT
        DISTINCT block_id
    FROM
        {% if target.database == 'SOLANA' %}
        solana.silver.votes
        {% else %}
        solana_dev.silver.votes
        {% endif %}
    WHERE
        block_id >= 306000000
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
    LEFT OUTER JOIN {{ ref('streamline__complete_block_txs_2') }} cmp2
    ON m.block_id = cmp2.block_id
WHERE
    (
        m.block_id < {{ cutover_block_id }} -- cutover block id from silver__transactions
        AND (cmp.error IS NOT NULL OR cmp.block_id IS NULL)
    )
    OR (
        m.block_id >= {{ cutover_block_id }}
        AND cmp2._partition_id >= 150000
        AND (cmp2.block_id IS NULL)
    )
