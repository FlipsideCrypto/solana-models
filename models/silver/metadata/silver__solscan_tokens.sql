{{ config(
    materialized = 'incremental',
    unique_key = "token_address",
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['scheduled_non_core']
) }}

{% set legacy_solscan_data_cutoff_date = '2024-11-11' %}

WITH base AS (
    SELECT
        value:"mintAddress"::STRING AS token_address,
        value:extensions:coingeckoId::STRING AS coingecko_id,
        value:tokenName::STRING AS name,
        value:tokenSymbol::STRING AS symbol,
        value:decimals::INT AS decimals,
        value:tag AS tags,
        value:icon::STRING AS icon,
        value:twitter::STRING AS twitter,
        value:website::STRING AS website,
        _inserted_timestamp
    FROM 
        {{ source('bronze_api', 'solscan_token_list') }},
        LATERAL flatten(data:data:data)
    WHERE
        token_address IS NOT NULL 
        AND _inserted_timestamp < '{{ legacy_solscan_data_cutoff_date }}'
        {% if is_incremental() %}
        AND _inserted_timestamp >= (
            SELECT
                max(_inserted_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
    UNION ALL
    SELECT
        address AS token_address,
        data:data:"coingeckoId"::STRING AS coingecko_id,
        data:data:name::STRING AS name,
        data:data:symbol::STRING AS symbol,
        data:data:decimals::INT AS decimals,
        data:data:tag AS tags,
        data:data:icon::STRING AS icon,
        data:data:twitter::STRING AS twitter,
        data:data:website::STRING AS website,
        _inserted_timestamp
    FROM
        {{ source('bronze_api', 'token_metadata') }}
    {% if is_incremental() %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                max(_inserted_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
    UNION ALL
    /* we no longer have coingecko_id, tags, icon, twitter, website returned by the v2 solscan api 
       join to the existing data to continue to return this data on incrementals 
       otherwise it will be NULL for new tokens */
    SELECT
        d.value:address::STRING AS token_address,
        e.coingecko_id AS coingecko_id,
        d.value:name::STRING AS name,
        d.value:symbol::STRING AS symbol,
        d.value:decimals::INT AS decimals,
        e.tags AS tags,
        e.icon AS icon,
        e.twitter AS twitter,
        e.website AS website,
        t._inserted_timestamp
    FROM
        {{ ref('bronze__streamline_solscan_token_list') }} AS t
    JOIN 
        table(flatten(data:data)) AS d
    {% if is_incremental() %}
    LEFT JOIN 
        {{ this }} AS e
        ON d.value:address::STRING = e.token_address
    {% else %}
    LEFT JOIN 
        (
            SELECT 
                NULL AS token_address,
                NULL AS coingecko_id,
                NULL AS name,
                NULL AS symbol,
                NULL AS decimals,
                NULL AS tags,
                NULL AS icon,
                NULL AS twitter,
                NULL AS website,
                NULL AS _inserted_timestamp
        ) AS e
        ON d.value:address::STRING = e.token_address
    {% endif %}
    WHERE
        d.value:address::STRING IS NOT NULL
        AND t._inserted_timestamp >= '{{ legacy_solscan_data_cutoff_date }}'
        {% if is_incremental() %}
        AND t._inserted_timestamp >= (
            SELECT
                max(_inserted_timestamp)
            FROM
                {{ this }}
        )
        {% endif %}
)
SELECT
    token_address,
    coingecko_id,
    name,
    symbol,
    decimals,
    tags,
    icon,
    twitter,
    website,
    _inserted_timestamp,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM 
    base 
QUALIFY
    row_number() OVER (
        PARTITION BY token_address
        ORDER BY (coingecko_id IS NOT NULL) DESC, _inserted_timestamp DESC
    ) = 1