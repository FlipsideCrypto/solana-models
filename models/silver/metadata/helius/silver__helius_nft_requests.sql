{{ config(
    materialized = 'incremental',
    unique_key = "_id",
    tags = ['helius']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver__nft_mints') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(max_mint_event_inserted_timestamp)
        FROM
            {{ this }}
    )
    AND _inserted_timestamp <= (
        SELECT
            MAX(max_mint_event_inserted_timestamp) :: DATE + 3
        FROM
            {{ this }}
    )
{% else %}
WHERE
    _inserted_timestamp :: DATE = '2022-08-12'
{% endif %}
),
numbered AS (
    SELECT
        *,
        ROW_NUMBER() over (
            ORDER BY
                _inserted_timestamp
        ) AS row_num
    FROM
        base qualify(ROW_NUMBER() over (PARTITION BY mint
    ORDER BY
        _inserted_timestamp DESC)) = 1
),
grouped AS (
    SELECT
        mint,
        FLOOR((row_num - 1) / 400) + 1 AS group_num,
        _inserted_timestamp
    FROM
        numbered
    ORDER BY
        row_num
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
    livequery_dev.utils.udf_json_rpc_call(
        'getAssetBatch',
        OBJECT_CONSTRUCT(
            'ids',
            list_mint
        )
    ) AS rpc_request,
    max_mint_event_inserted_timestamp,
    concat_ws('-',rpc_request:id,max_mint_event_inserted_timestamp) as _id
FROM
    list_mints
GROUP BY
    group_num,
    max_mint_event_inserted_timestamp,
    list_mint
