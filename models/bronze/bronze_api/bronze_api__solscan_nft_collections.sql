{{ config(
    materialized = 'incremental',
    tags = ['solscan']
) }}

WITH collections AS (

    SELECT
        mint,
        group_value AS collection_id,
        _inserted_timestamp AS helius_inserted_timestamp,
        ROW_NUMBER() over (
            PARTITION BY collection_id
            ORDER BY
                _inserted_timestamp
        ) AS temp_rn
    FROM
        {{ ref('silver__helius_nft_metadata') }}
    WHERE
        group_key = 'collection'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(helius_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND _inserted_timestamp :: DATE = '2023-10-03'
{% endif %}
),
distinct_collections AS (
    SELECT
        mint,
        collection_id,
        helius_inserted_timestamp,
        ROW_NUMBER() over (
            ORDER BY
                helius_inserted_timestamp
        ) AS rn
    FROM
        collections
    WHERE
        temp_rn = 1
),
response AS ({% for item in range(1, 100) %}
    (
    SELECT
        mint, 
        collection_id, 
        helius_inserted_timestamp, 
        livequery_dev.live.udf_api('GET', 'https://pro-api.solscan.io/v1.0/nft/token/info/' || (mint), OBJECT_CONSTRUCT('Accept', 'application/json', 'token', (
            SELECT
                api_key
            FROM
                crosschain.silver.apis_keys
            WHERE
                api_name = 'solscan')),{}) AS DATA, 
        SYSDATE() AS _inserted_timestamp
    FROM
        distinct_collections
WHERE
    rn = {{ item }}) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %})
SELECT
    collection_id,
    DATA,
    helius_inserted_timestamp,
    _inserted_timestamp
FROM
    response
