{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"block_rewards",
        "sql_limit" :"100000",
        "producer_batch_size" :"100000",
        "worker_batch_size" :"12500",
        "exploded_key": tojson(["result.rewards"]),
        "sql_source" :"{{this.identifier}}" }
    )
) }}

{% if execute %}
    {% set next_batch_num_query %}
    SELECT
        greatest(
            (SELECT coalesce(max(_partition_id),0) FROM {{ ref('streamline__complete_block_rewards') }}),
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
    /* TODO diff with completed */
    WHERE
        block_id = 267408004
    /*EXCEPT
    SELECT
        block_id
    FROM
        {{ ref('streamline__complete_block_rewards') }}
    EXCEPT
    SELECT 
        block_id
    FROM
        {{ ref('streamline__complete_block_rewards_2') }}*/
)
SELECT
    block_id,
    'batch=2' AS partition_key,
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