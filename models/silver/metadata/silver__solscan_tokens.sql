{{ config(
    materialized = 'incremental',
    unique_key = "token_address",
    incremental_strategy = 'merge'
) }}

WITH base AS (

    SELECT
        VALUE :"address" :: STRING AS token_address,
        VALUE :extensions :coingeckoId :: STRING AS coingecko_id,
        VALUE :tokenName :: STRING AS NAME,
        VALUE :tokenSymbol :: STRING AS symbol,
        VALUE :decimals :: INT AS decimals,
        VALUE :tag AS tags,
        VALUE :icon :: STRING AS icon,
        VALUE :twitter :: STRING AS twitter,
        VALUE: website :: STRING AS website,
        _inserted_timestamp
    FROM
        {{ source(
            'bronze_api',
            'solscan_token_list'
        ) }},
        LATERAL FLATTEN(DATA :data :data)

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
UNION ALL
SELECT
    address AS token_address,
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
    {{ source(
        'bronze_api',
        'token_metadata'
    ) }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
)
SELECT
    token_address,
    coingecko_id,
    NAME,
    symbol,
    decimals,
    tags,
    icon,
    twitter,
    website,
    _inserted_timestamp
FROM
    base qualify(ROW_NUMBER() over (PARTITION BY token_address
ORDER BY
    _inserted_timestamp DESC)) = 1
