{{
    config(
        tags=["test_hourly"]
    )
}}

WITH max_loaded_part as (
    SELECT 
        LEAST(
            (SELECT 
                max(_partition_id)
            FROM 
                {{ ref('silver__transactions') }}
            ),
            (SELECT 
                max(_partition_id)
            FROM 
                {% if target.database == 'SOLANA' %}
                solana.silver.votes
                {% else %}
                solana_dev.silver.votes
                {% endif %}
            )) as max_partition_id  
)
, base AS (
    SELECT
        block_id,
        _partition_id,
        tx_id
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        _partition_id between (select max_partition_id-10 from max_loaded_part) and (select max_partition_id from max_loaded_part)
    GROUP BY
        1,2,3
    UNION
    SELECT
        block_id,
        _partition_id,
        tx_id
    FROM
        {% if target.database == 'SOLANA' %}
        solana.silver.votes
        {% else %}
        solana_dev.silver.votes
        {% endif %}
    WHERE
        _partition_id between (select max_partition_id-10 from max_loaded_part) and (select max_partition_id from max_loaded_part)
    GROUP BY
        1,2,3
),
C AS (
    SELECT
        b.block_id,
        b._partition_id,
        COUNT(distinct tx_id)
    FROM
        {% if target.database == 'SOLANA' %}
        solana.bronze.transactions2
        {% else %}
        solana_dev.bronze.transactions2
        {% endif %} b
    INNER JOIN 
        {% if target.database == 'SOLANA' %}
        solana.streamline.complete_block_txs
        {% else %}
        solana_dev.streamline.complete_block_txs
        {% endif %} C
        ON C._partition_id = b._partition_id
        AND C.block_id = b.block_id
    WHERE
        b._partition_id between (select max_partition_id-10 from max_loaded_part) and (select max_partition_id from max_loaded_part)
        AND b.error IS NULL
        AND b.tx_id IS NOT NULL
    GROUP BY
        1,2
    EXCEPT
    SELECT
        block_id,
        _partition_id,
        count(tx_id)
    FROM
        base
    GROUP BY
        1,2
)
SELECT
    DISTINCT block_id
FROM
    C
WHERE
    _partition_id <= (
        SELECT
            MAX(_partition_id)
        FROM
            {% if target.database == 'SOLANA' %}
            solana.silver.transactions
            {% else %}
            solana_dev.silver.transactions
            {% endif %}
    )
