{{ config(
    materialized = 'incremental'
) }}

with
    requests AS (
    SELECT
        *,
        FLOOR((ROW_NUMBER() OVER (ORDER BY max_mint_event_inserted_timestamp) - 1) / 2) + 1 AS batch_id
        -- (ROW_NUMBER() OVER (ORDER BY max_mint_event_inserted_timestamp)) AS batch_id
    FROM
        solana_dev.silver.helius_nft_requests
{% if is_incremental() %}
WHERE
    max_mint_event_inserted_timestamp > (
        SELECT
            max(max_mint_event_inserted_timestamp) from
                    {{ this }}
        )
        AND max_mint_event_inserted_timestamp < (
        SELECT
            max(max_mint_event_inserted_timestamp)::date + 2
            FROM
                {{ this }}
        ) 
    {% else %}
    where max_mint_event_inserted_timestamp::date = '2022-10-18'
    {% endif %}
),

response AS ({% for item in range(0,100) %}
    (
SELECT
ethereum.streamline.udf_json_rpc_call(
  'https://rpc.helius.xyz/?api-key=' || (
    SELECT
      api_key
    FROM
      crosschain.silver.apis_keys
    WHERE
      api_name = 'helius'
  ),{},
  calls
) AS DATA,
max_mint_event_inserted_timestamp,
SYSDATE() AS _inserted_timestamp
FROM
requests
WHERE
    batch_id = {{ item }}) 
{% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})

select * from response
