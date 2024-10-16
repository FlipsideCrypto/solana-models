{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"blocks_2",
        "sql_limit" :"25000",
        "producer_batch_size" :"25000",
        "worker_batch_size" :"10000",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "block_id", }
    )
) }}

{% if execute %}
    {% set next_batch_num_query %}
    SELECT
        greatest(
            295976123,
            (SELECT coalesce(max(block_id),0) FROM {{ ref('streamline__complete_blocks_2') }})
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
        {{ source('solana_streamline', 'complete_blocks') }}
    WHERE
        block_id <= 295976123
    EXCEPT
    SELECT 
        block_id
    FROM
        {{ ref('streamline__complete_blocks_2') }}
)
SELECT
    block_id,
    replace(current_date::string,'-','_') AS partition_key, -- Issue with streamline handling `-` in partition key so changing to `_`
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
