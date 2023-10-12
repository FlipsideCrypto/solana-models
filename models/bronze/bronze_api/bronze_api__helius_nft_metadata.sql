{{ config(
    materialized = 'incremental',
    unique_key = '_id',
    full_refresh = false,
    tags = ['helius']
) }}

WITH pre_requests AS (

    SELECT
        rpc_request,
        max_mint_event_inserted_timestamp,
        0 AS retry_count,
        _id
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
UNION ALL
-- failed requests
SELECT
    b.rpc_request,
    b.max_mint_event_inserted_timestamp,
    A.retry_count + 1 AS retry_count,
    b._id
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
    ON A._id = b._id
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
response AS ({% for item in range(1, 51) %}
    (
    SELECT
        livequery_dev.live.udf_api('POST', 'https://rpc.helius.xyz/?api-key=' || (
        SELECT
            api_key
        FROM
            {{ source(
                'crosschain_silver',
                'apis_keys'
            ) }}
        WHERE
            api_name = 'helius'
            ),
            {}, 
            rpc_request) AS DATA, 
        max_mint_event_inserted_timestamp,
        retry_count,
        _id,
        SYSDATE() AS _inserted_timestamp

    FROM
        final_requests
WHERE
    batch_id = {{ item }}) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    data,
    max_mint_event_inserted_timestamp,
    retry_count,
    DATA :status_code :: INT AS status_code,
    IFF(
        status_code = 200,
        TRUE,
        FALSE
    ) AS call_succeeded,
    _id,
    _inserted_timestamp
FROM
    response
