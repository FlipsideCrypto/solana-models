{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ 
            "external_table" :"validators_list_2",
            "sql_limit" :"1",
            "producer_batch_size" :"1",
            "worker_batch_size" :"1",
            "sql_source" :"{{this.identifier}}",
        }
    )
) }}

SELECT
    replace(current_date::string,'-','_') AS partition_key, -- Issue with streamline handling `-` in partition key so changing to `_`
    '{{ invocation_id }}' AS invocation_id,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/validators/mainnet.json',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json',
            'Token',
            '{Authentication}'
        ),
        {},
        'Vault/prod/solana/validators_app/api'
    ) AS request
