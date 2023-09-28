{{ config(
    materialized = 'incremental',
    full_refresh = false,
    tags = ['helius']
) }}

WITH requests AS (

    SELECT
        *,
        -- FLOOR((ROW_NUMBER() over (ORDER BY max_mint_event_inserted_timestamp) - 1) / 2) + 1 AS batch_id
        (ROW_NUMBER() OVER (ORDER BY max_mint_event_inserted_timestamp)) AS batch_id
    FROM
        {{ ref('silver__helius_nft_requests') }}

{% if is_incremental() %}
WHERE
    max_mint_event_inserted_timestamp > (
        SELECT
            MAX(max_mint_event_inserted_timestamp)
        FROM
            {{ this }}
    )
    AND max_mint_event_inserted_timestamp < (
        SELECT
            MAX(max_mint_event_inserted_timestamp) :: DATE + 3
        FROM
            {{ this }}
    )
{% else %}
WHERE
    max_mint_event_inserted_timestamp :: DATE = '2022-10-18'
{% endif %}
),
response AS ({% for item in range(0, 100) %}
    (
     SELECT
        livequery.live.udf_api(
            'POST',
            'https://rpc.helius.xyz/?api-key=' || (
                SELECT
                    api_key
                FROM
                    crosschain.silver.apis_keys
                WHERE
                    api_name = 'helius'),
            {},
            rpc_request
        ) AS data,
        max_mint_event_inserted_timestamp,
        SYSDATE() AS _inserted_timestamp
     FROM
    requests
WHERE
    batch_id = {{ item }}) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    *
FROM
    response
