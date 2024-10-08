{{
    config(
        materialized="view",
        post_hook = fsc_utils.if_data_call_function_v2(
            func = 'streamline.udf_bulk_rest_api_v2',
            target = "{{this.schema}}.{{this.identifier}}",
            params ={ "external_table" :"helius_nft_metadata",
            "sql_limit" :"1",
            "producer_batch_size" :"1",
            "worker_batch_size" :"1",
            "sql_source" :"{{this.identifier}}",
            "exploded_key": tojson(["result"]), }
        )
    )
}}

with numbered AS (
    SELECT
        *,
        ROW_NUMBER() over (ORDER BY _inserted_timestamp) AS row_num
    FROM
        {{ ref('silver__nft_compressed_mints') }}
    WHERE 
        _inserted_timestamp >= (SELECT coalesce(max(max_mint_event_inserted_timestamp),'2000-01-01') FROM {{ ref('streamline__complete_helius_nft_metadata_requests') }})
),
grouped AS (
    SELECT 
        mint,
        FLOOR((row_num - 1) / 1000) + 1 AS group_num,
        _inserted_timestamp
    FROM
        numbered
),
list_mints AS (
    SELECT
        ARRAY_AGG(mint) AS list_mint,
        MAX(_inserted_timestamp) AS max_mint_event_inserted_timestamp,
        group_num
    FROM
        grouped
    GROUP BY
        group_num
)
SELECT
    concat_ws('_',max_mint_event_inserted_timestamp,group_num) AS helius_nft_metadata_requests_id,
    max_mint_event_inserted_timestamp::string AS max_mint_event_inserted_timestamp,
    replace(current_date::string,'-','_') AS partition_key, -- Issue with streamline handling `-` in partition key so changing to `_`
    {{ target.database }}.live.udf_api(
        'POST',
        '{service}/?api-key={Authentication}',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),
        OBJECT_CONSTRUCT(
            'id',
            helius_nft_metadata_requests_id,
            'jsonrpc',
            '2.0',
            'method',
            'getAssetBatch',
            'params',
            OBJECT_CONSTRUCT(
                'ids',
                list_mint
            )
        ),
        'Vault/prod/solana/helius/mainnet'
    ) AS request
FROM
    list_mints
ORDER BY
    group_num




-- SELECT
--     utils.udf_json_rpc_call(
--         'getAssetBatch',
--         OBJECT_CONSTRUCT(
--             'ids',
--             list_mint
--         )
--     ) AS rpc_request,
--     max_mint_event_inserted_timestamp,
--     concat_ws('-',rpc_request:id,max_mint_event_inserted_timestamp) as _id
-- FROM
--     list_mints
-- WHERE
--     group_num = 1
-- GROUP BY
--     group_num,
--     max_mint_event_inserted_timestamp,
--     list_mint;

-- SELECT
--     solana.live.udf_api('POST', 'https://rpc.helius.xyz/?api-key=' || (
--         SELECT
--             api_key
--         FROM
--             crosschain.silver.apis_keys
--         WHERE
--             api_name = 'helius'
--     ),
--     {}, 
--     rpc_request) AS DATA, 
--     max_mint_event_inserted_timestamp,
--     -- retry_count,
--     -- _id,
--     SYSDATE() AS _inserted_timestamp

-- FROM
--     table(result_scan('01b67a58-050b-17e1-3d4f-830257967607'));