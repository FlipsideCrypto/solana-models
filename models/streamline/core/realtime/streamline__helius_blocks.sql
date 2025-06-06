{{ config(
    materialized = 'view',
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"helius_blocks",
            "sql_limit" :"10000",
            "producer_batch_size" :"1000",
            "worker_batch_size" :"1000",
            "sql_source" :"{{this.identifier}}", 
        }
    )
) }}

{% set producer_limit_size = 10000 %}

WITH base AS (
    SELECT 
        b.block_id
    FROM 
        {{ ref('silver__blocks') }} b
    LEFT OUTER JOIN 
        {{ source('solana_silver','_blocks_tx_count') }} b2
        ON b.block_id = b2.block_id
    WHERE 
        b.block_id >= 226000000
        AND (b2.block_id IS NULL 
            OR b2.transaction_count IS NULL)
        AND b.block_timestamp::DATE <= current_date
),
block_ids AS (
    SELECT 
        block_id 
    FROM 
        base b
    QUALIFY 
        row_number() OVER (ORDER BY b.block_id DESC) <= {{ producer_limit_size }}
)
SELECT
    block_id,
    replace(current_date::string,'-','_') AS partition_key, -- Issue with streamline handling `-` in partition key so changing to `_`
    {{ target.database }}.live.udf_api(
        'POST',
        '{Service}?apikey={Authentication}',
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
                    'signatures',
                    'maxSupportedTransactionVersion',
                    0
                )
            )
        ),
        'Vault/prod/solana/ankr/mainnet'
    ) AS request
FROM
    block_ids