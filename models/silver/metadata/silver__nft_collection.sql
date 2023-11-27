{{ config(
    materialized = 'incremental',
    unique_key = "collection_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['nft_api']
) }}

with collections as (
SELECT
    items.value ['grouping'] AS GROUPING,
    grouping[0]:group_key::string AS group_key,
    grouping[0]:group_value::string AS collection_id,
    ROW_NUMBER() over (
            PARTITION BY collection_id
            ORDER BY
                _inserted_timestamp
        ) AS temp_rn,
    _inserted_timestamp
FROM
    {{ ref('bronze_api__helius_nft_metadata') }},
    LATERAL FLATTEN(
        input => DATA :data :result
    ) AS items
WHERE
    group_key = 'collection'
{% if is_incremental() %}
AND
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
    AND _inserted_timestamp :: DATE = '2023-10-03'
{% endif %}
    ),
distinct_collections AS (
    SELECT
        collection_id,
        _inserted_timestamp,
                ROW_NUMBER() over (
            ORDER BY
                _inserted_timestamp
        ) AS rn
    FROM
        collections
    WHERE
        temp_rn = 1
{% if is_incremental() %}
AND
    collection_id not in (
        SELECT
            collection_id
        FROM
            {{ this }}
    )
{% else %}
    AND _inserted_timestamp :: DATE = '2023-10-03'
{% endif %}
qualify(ROW_NUMBER() over (ORDER BY _inserted_timestamp)) <= 150

),
response AS (
    {% for batch in range(1, 11) %} -- 10 iterations
        (
        SELECT
            collection_id, 
            _inserted_timestamp, 
            live.udf_api('GET', 'https://pro-api.solscan.io/v1.0/nft/token/info/' || (collection_id), OBJECT_CONSTRUCT('Accept', 'application/json', 'token', (
                SELECT
                    api_key
                FROM
                    crosschain.silver.apis_keys
                WHERE
                    api_name = 'solscan')),{}) AS DATA
        FROM
            distinct_collections
        WHERE
            rn BETWEEN {{ (batch - 1) * 15 + 1 }} AND {{ batch * 15 }}
        )
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
)
SELECT
    collection_id,
    CASE 
        WHEN DATA :data :data [0] :nft_collection_name :: STRING = '' 
        THEN NULL 
        ELSE DATA :data :data [0] :nft_collection_name :: STRING 
    END AS nft_collection_name,
    CASE 
        WHEN DATA :data :data [0] :nft_collection_id :: STRING = '' 
        THEN NULL 
        ELSE DATA :data :data [0] :nft_collection_id :: STRING 
    END AS solscan_collection_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['collection_id']) }} AS nft_collection_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    response
