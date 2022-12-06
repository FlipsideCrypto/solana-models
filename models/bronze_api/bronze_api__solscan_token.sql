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
        SYSDATE() _inserted_timestamp
    FROM
        (
            SELECT
                DISTINCT swap_from_mint mint
            FROM
                {{ ref('core__fact_swaps') }}
            WHERE
                swap_from_mint IS NOT NULL

{% if is_incremental() %}
EXCEPT
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
{% endif %}
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
    _inserted_timestamp
FROM
    base
