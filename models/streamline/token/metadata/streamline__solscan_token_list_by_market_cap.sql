{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ 
            "external_table" :"solscan_token_list",
            "sql_limit" :"250",
            "producer_batch_size" :"250",
            "worker_batch_size" :"250",
            "sql_source" :"{{this.identifier}}",
        }
    )
) }}

WITH page_numbers AS (
    SELECT
        SEQ4()+1 AS page_number
    FROM
        TABLE(GENERATOR(rowcount => 250))
)
SELECT
    page_number,
    replace(current_date::string,'-','_') AS partition_key, -- Issue with streamline handling `-` in partition key so changing to `_`
    live.udf_api(
        'GET',
        concat('{Service}/token/list?sort_by=market_cap&sort_order=desc&page_size=40&page=',page_number),
        object_construct(
            'Content-Type',
            'application/json',
            'token',
            '{Authentication}'
        ),
        {},
        'Vault/prod/solana/solscan/v2'
    ) AS request
FROM
    page_numbers
