{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ 
            "external_table" :"stake_program_accounts_2",
            "sql_limit" :"100000",
            "producer_batch_size" :"20000",
            "worker_batch_size" :"1000",
            "sql_source" :"{{this.identifier}}",
            "order_by_column": "group_num",
        }
    )
) }}

WITH base AS (
    SELECT 
        block_timestamp,
        account_address,
    FROM 
        {{ ref('streamline__stake_account_states') }}
    WHERE 
        is_active
        OR (NOT is_active AND block_timestamp >= current_date - 2)
),
numbered_rows AS (
    SELECT 
        account_address,
        row_number() OVER (ORDER BY block_timestamp) AS row_num
    FROM
        base
),
grouped_rows AS (
    SELECT
        account_address,
        floor((row_num - 1) / 100) AS group_num
    FROM
        numbered_rows
),
agg AS (
    SELECT 
        group_num,
        array_agg(account_address) as accounts_requested
    FROM
        grouped_rows
    GROUP BY
        group_num
)
SELECT
    replace(current_date::string,'-','_') AS partition_key, -- Issue with streamline handling `-` in partition key so changing to `_`
    accounts_requested::string AS accounts_requested,
    group_num,
    '{{ invocation_id }}' AS invocation_id,
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/{Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            1,
            'jsonrpc',
            '2.0',
            'method',
            'getMultipleAccounts',
            'params',
            ARRAY_CONSTRUCT(
                accounts_requested,
                OBJECT_CONSTRUCT(
                    'encoding',
                    'jsonParsed'
                )
            )
        ),
        'Vault/prod/solana/quicknode/mainnet'
    ) AS request
FROM
    agg
