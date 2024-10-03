{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"block_rewards_2",
        "sql_limit" :"100000",
        "producer_batch_size" :"100000",
        "worker_batch_size" :"12500",
        "exploded_key": tojson(["result.rewards"]),
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "block_id", }
    )
) }}

{% if execute %}
    {% set next_batch_num_query %}
    SELECT
        greatest(
            59890, /* cutoff partition id in PROD after deploy */
            (SELECT coalesce(max(_partition_id),0) FROM {{ ref('streamline__complete_block_rewards_2') }})
        )+1
    {% endset %}
    {% set next_batch_num = run_query(next_batch_num_query)[0][0] %}
{% endif %}

WITH blocks AS (
    SELECT
        block_id
    FROM
        {{ ref("streamline__blocks") }}
    EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__complete_block_rewards') }}
    WHERE
        block_id <= 292334107 /* cutoff block_id in PROD after deploy */
    EXCEPT
    SELECT 
        block_id
    FROM
        {{ ref('streamline__complete_block_rewards_2') }}
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
