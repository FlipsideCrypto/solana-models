{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"block_txs_2",
        "sql_limit" :"10000",
        "producer_batch_size" :"10000",
        "worker_batch_size" :"200",
        "async_concurrent_requests": "10",
        "exploded_key": tojson(["result.transactions"]),
        "include_top_level_json": tojson(["result.blockTime"]),
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "block_id", }
    )
) }}

{% if execute %}
    {% set next_batch_num_query %}
    SELECT
        greatest(
            150000, /* This partition id needs to start at a value greater than the current max partition of the legacy streamline data */
            (SELECT coalesce(max(_partition_id),0) FROM {{ ref('streamline__complete_block_txs_2') }})
        )+1
    {% endset %}
    {% set next_batch_num = run_query(next_batch_num_query)[0][0] %}
{% endif %}

WITH blocks_base AS (
    SELECT
        block_id
    FROM
        {{ ref("streamline__blocks") }}
    WHERE
        block_id >= 305806921
    EXCEPT
    SELECT
        block_id
    FROM
        {{ source('solana_streamline', 'complete_block_txs') }}
    WHERE
        block_id <= 305806921 /* cutoff block_id in PROD after deploy */
    EXCEPT
    SELECT 
        block_id
    FROM
        {{ ref('streamline__complete_block_txs_2') }}
),
solscan_discrepancy_retries AS (
    SELECT
        m.block_id
    FROM
        {{ ref('streamline__transactions_and_votes_missing_7_days') }} m
    LEFT JOIN 
        {{ ref('streamline__complete_block_txs_2') }} C
        ON C.block_id = m.block_id
    WHERE
        C._partition_id <= m._partition_id
    LIMIT 200
),
blocks AS (
    SELECT
        *
    FROM
        blocks_base
    UNION ALL
    SELECT
        *
    FROM
        solscan_discrepancy_retries
)
SELECT
    block_id,
    'batch={{next_batch_num}}' AS partition_key,
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            block_id,
            'jsonrpc',
            '2.0',
            'method',
            'getBlock',
            'params',
            ARRAY_CONSTRUCT(
                block_id,
                OBJECT_CONSTRUCT(
                    'encoding',
                    'jsonParsed',
                    'rewards',
                    False,
                    'transactionDetails',
                    'full',
                    'maxSupportedTransactionVersion',
                    0
                )
            )
        ),
        'Vault/prod/solana/quicknode/mainnet'
    ) AS request
FROM
    blocks
