/* TODO add exploded key */
{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"block_rewards",
        "sql_limit" :"100000",
        "producer_batch_size" :"100000",
        "worker_batch_size" :"12500",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["result.data"]) }
    )
) }}

WITH blocks AS (
    SELECT
        block_id
    FROM
        {{ ref("streamline__blocks") }}
    /* TODO diff with completed */
    WHERE
        block_id >= 275322345
    LIMIT 1
)
SELECT
    /* TODO use completed to get next batch num */
    'batch=1' AS partition_key,
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
                    True,
                    'transactionDetails',
                    'none',
                    'maxSupportedTransactionVersion',
                    0
                )
            )
        ),
        'Vault/prod/solana/quicknode/mainnet'
    ) AS request
FROM
    blocks
ORDER BY
    block_id