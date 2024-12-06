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
            129041, /* cutoff partition id in PROD after deploy */
            (SELECT coalesce(max(_partition_id),0) FROM {{ ref('streamline__complete_block_txs_2') }})
        )+1
    {% endset %}
    {% set next_batch_num = run_query(next_batch_num_query)[0][0] %}
{% endif %}

WITH blocks AS (
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
