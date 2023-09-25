{{ config(
    materialized = 'incremental',
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
    block_timestamp :: DATE = '2022-10-18'
{% endif %}
ORDER BY
    _inserted_timestamp ASC
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
    ARRAY_AGG(
        { 'id': CONCAT(
            group_num,
            '-',
            max_mint_event_inserted_timestamp :: DATE
        ),
        'jsonrpc': '2.0',
        'method': 'getAssetBatch',
        'params':{ 'ids': list_mint }}
    ) calls,
    -- group_num AS request_num,
    max_mint_event_inserted_timestamp
FROM
    list_mints
GROUP BY
    group_num,
    max_mint_event_inserted_timestamp
