{{ config(
    materialized = 'incremental',
    full_refresh = false
) }}

WITH base AS (

    SELECT
        mint,
        ethereum.streamline.udf_api(
            'GET',
            'https://public-api.solscan.io/token/meta?tokenAddress=' || mint,{},{}
        ) AS DATA,
        SYSDATE() _inserted_timestamp,
        _inserted_timestamp AS _inserted_timestamp_from_swap
    FROM
        (
            SELECT
                A.swap_from_mint AS mint,
                MIN(
                    A._inserted_timestamp
                ) _inserted_timestamp
            FROM
                {{ ref('silver__swaps') }} A

{% if is_incremental() %}
LEFT JOIN (
    SELECT
        mint
    FROM
        (
            SELECT
                mint
            FROM
                {{ this }}
            UNION ALL
            SELECT
                token_address
            FROM
                silver.solscan_tokens
        )
) b
ON A.swap_from_mint = b.mint
{% endif %}
WHERE
    A.swap_from_mint IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp :: DATE >= (
    SELECT
        MAX(
            _inserted_timestamp_from_swap :: DATE
        )
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    A.swap_from_mint
LIMIT
    1
)
)
SELECT
    mint,
    DATA :data :"address" :: STRING AS token_address,
    DATA :data :"coingeckoId" :: STRING AS coingecko_Id,
    DATA :data :name :: STRING AS NAME,
    DATA :data :symbol :: STRING AS symbol,
    DATA :data :decimals :: INT AS decimals,
    DATA :data :tag AS tags,
    DATA :data :icon :: STRING AS icon,
    DATA :data :twitter :: STRING AS twitter,
    DATA :data :website :: STRING AS website,
    _inserted_timestamp,
    _inserted_timestamp_from_swap
FROM
    base
