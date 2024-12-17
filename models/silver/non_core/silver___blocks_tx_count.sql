{{ config(
    materialized = 'incremental',
    unique_key = ['block_id'],
) }}

WITH solscan_blocks AS (
    SELECT
        block_id,
        coalesce(data:data:transactions_count,data:result:transactionCount) AS solscan_transaction_count,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_solscan_blocks_2') }}
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                max(_inserted_timestamp) - INTERVAL '15 MINUTE'
            FROM
                {{ this }}
        )
    {% endif %}
),
helius_blocks AS (
    SELECT
        block_id,
        transaction_count AS helius_transaction_count,
        _inserted_timestamp
    FROM
        {{ ref('bronze__streamline_helius_blocks') }}
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                max(_inserted_timestamp) - INTERVAL '15 MINUTE'
            FROM
                {{ this }}
        )
    {% endif %}
)
SELECT
    block_id,
    coalesce(s.solscan_transaction_count,helius_transaction_count) AS transaction_count,
    least_ignore_nulls(s._inserted_timestamp,h._inserted_timestamp) AS _inserted_timestamp
FROM
    solscan_blocks AS s
FULL OUTER JOIN
    helius_blocks AS h
    USING (block_id)
QUALIFY 
    row_number() over (partition by block_id order by least_ignore_nulls(s._inserted_timestamp,h._inserted_timestamp) desc, transaction_count desc) = 1