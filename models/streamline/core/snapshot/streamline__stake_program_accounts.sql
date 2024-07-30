{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"stake_program_accounts_2",
        "sql_limit" :"10",
        "producer_batch_size" :"10",
        "worker_batch_size" :"10",
        "sql_source" :"{{this.identifier}}",
        "exploded_key": tojson(["result.value"])},
    )
) }}

WITH base AS (
    SELECT *
    from table(result_scan('01b5ff0a-0507-cd35-3d4f-830245fa1833'))
    where is_delegated
    limit 10
),
agg AS (
    select 
        array_agg(stake_account) as accounts_requested
    from
        base
)
SELECT
    replace(current_date::string,'-','/') AS partition_key,
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
