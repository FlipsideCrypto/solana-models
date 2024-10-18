{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params = { 
            "external_table": "validator_vote_program_accounts_2",
            "sql_limit": "1",
            "producer_batch_size": "1",
            "worker_batch_size": "1",
            "sql_source": "{{this.identifier}}",
            "exploded_key": tojson(["result.value"]),
        }
    )
) }}

SELECT
    replace(current_date::string, '-', '_') AS partition_key, -- Issue with streamline handling `-` in partition key so changing to `_`
    '{{ invocation_id }}' AS invocation_id,
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/{Authentication}',
        object_construct(
            'Content-Type',
            'application/json'
        ),
        object_construct(
            'id',
            1,
            'jsonrpc',
            '2.0',
            'method',
            'getProgramAccounts',
            'params',
            array_construct(
                'Vote111111111111111111111111111111111111111',
                object_construct(
                    'commitment',
                    'confirmed',
                    'encoding',
                    'jsonParsed',
                    'withContext',
                    True
                )
            )
        ),
        'Vault/prod/solana/quicknode/mainnet'
    ) AS request
