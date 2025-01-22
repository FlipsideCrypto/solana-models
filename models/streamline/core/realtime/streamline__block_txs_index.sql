{{ config(
    materialized = 'view',
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"block_txs_index_backfill",
            "sql_limit" :"37500",
            "producer_batch_size" :"37500",
            "worker_batch_size" :"750",
            "sql_source" :"{{this.identifier}}", 
            "order_by_column": "block_id DESC",
            "exploded_key": tojson(["result.signatures"]),
            "include_top_level_json": tojson(["result.blockTime"]),
        }
    )
) }}

WITH block_ids AS (
    SELECT 
        b.block_id
    FROM 
        {{ ref('silver__blocks') }} b
    WHERE 
        -- all blocks after this should have tx id filled in already so start the backfill here
        b.block_id <= 307868470 
    EXCEPT
    SELECT DISTINCT
        block_id
    FROM
        {{ ref('silver__backfill_transactions_index') }}
)
SELECT
    block_id,
    to_char(dateadd(
        minute, 
        floor(date_part(minute, current_timestamp) / 15) * 15, 
        date_trunc('hour', current_timestamp)
    ), 'YYYY_MM_DD_HH24_MI') AS partition_key, -- Issue with streamline handling `-` in partition key so changing to `_`
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