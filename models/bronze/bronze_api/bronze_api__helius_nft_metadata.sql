{{ config(
    materialized = 'incremental',
    unique_key = 'max_mint_event_inserted_timestamp',
    full_refresh = false,
    tags = ['helius']
) }}

WITH pre_requests AS (

    SELECT
        *,
        0 AS retry_count
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
            MAX(max_mint_event_inserted_timestamp) :: DATE + 2
        FROM
            {{ this }}
    )
UNION ALL
-- failed requests
SELECT
    b.rpc_request,
    b.max_mint_event_inserted_timestamp,
    A.retry_count + 1 AS retry_count
FROM
    (
        SELECT
            *
        FROM
            {{ this }}
        WHERE
            NOT call_succeeded
            AND retry_count < 3
    ) A
    LEFT JOIN {{ ref('silver__helius_nft_requests') }}
    b
    ON A.max_mint_event_inserted_timestamp = b.max_mint_event_inserted_timestamp
{% else %}
WHERE
    max_mint_event_inserted_timestamp :: DATE = '2022-08-12'
{% endif %}
),
final_requests AS (
    SELECT
        *,(ROW_NUMBER() over (
    ORDER BY
        max_mint_event_inserted_timestamp)) AS batch_id
    FROM
        pre_requests
),
response AS ({% for item in range(0, 100) %}
    (
    SELECT
        livequery.live.udf_api('POST', 'https://rpc.helius.xyz/?api-key=' || (
        SELECT
            api_key
        FROM
            crosschain.silver.apis_keys
        WHERE
            api_name = 'helius'),
            {}, 
            rpc_request) AS DATA, 
        max_mint_event_inserted_timestamp,
        retry_count,
        SYSDATE() AS _inserted_timestamp

    FROM
        final_requests
WHERE
    batch_id = {{ item }}) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    *,
    (
        DATA :data :message IS NULL
    )
    AND (
        DATA :error IS NULL
    ) AS call_succeeded,
    COALESCE(
        DATA :data :code :: INT,
        DATA :status_code :: INT
    ) AS error_code
FROM
    response
