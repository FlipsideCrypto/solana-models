{{ config(
    materialized = 'view',
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"solscan_blocks_2",
        "sql_limit" :"10000",
        "producer_batch_size" :"1000",
        "worker_batch_size" :"1000",
        "sql_source" :"{{this.identifier}}" }
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
        AND b2.block_id IS NULL
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
    /* TODO: change vault secret path when the sync is fixed */
    live.udf_api(
        'GET',
        concat('{service}/block/',block_id),
        object_construct(
            'Content-Type',
            'application/json',
            'token',
            '{Authentication}'
        ),
        {},
        'Vault/prod/solana/solscan_v1'
    ) AS request
FROM
    block_ids