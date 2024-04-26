{{ config(
    materialized = 'incremental',
    unique_key = "token_address",
    incremental_strategy = 'merge',
    tags = ['scheduled_non_core']
) }}
-- this can be deprecated now? -- along with removing their sources
WITH cmc_base AS (

    SELECT
        VALUE :contract_address :: STRING AS token_address,
        NAME AS cmc_name,
        cmc_id,
        symbol AS cmc_symbol,
        metadata :description :: STRING AS cmc_description,
        metadata :logo :: STRING AS cmc_icon,
        metadata :tags AS cmc_tags,
        metadata :explorer :: STRING AS cmc_explorer,
        metadata :twitter AS cmc_twitter,
        metadata :website AS cmc_urls,
        _inserted_timestamp
    FROM
        {{ source(
            'crosschain_silver',
            'coin_market_cap_cryptocurrency_info'
        ) }},
        LATERAL FLATTEN(
            metadata :contract_address
        )
    WHERE
        VALUE :platform :name = 'Solana'
        AND COALESCE(
            token_address,
            ''
        ) <> ''
),
base AS (
    SELECT
        DISTINCT token_address
    FROM
        {{ ref('silver__solscan_tokens') }}
    WHERE
        COALESCE(token_address, '') <> ''

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION
SELECT
    DISTINCT token_address
FROM
    {{ source(
        'crosschain_silver',
        'asset_metadata_coin_gecko'
    ) }}
WHERE
    platform = 'solana'
    AND COALESCE(token_address, '') <> ''

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
UNION
SELECT
    DISTINCT token_address
FROM
    cmc_base

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
),
fin AS (
    SELECT
        A.token_address,
        COALESCE(
            cg.id,
            solscan.coingecko_id
        ) AS coin_gecko_id,
        cmc.cmc_id AS coin_market_cap_id,
        solscan.name AS ss_name,
        solscan.symbol AS ss_symbol,
        solscan.decimals AS ss_decimals,
        solscan.tags AS ss_tags,
        solscan.icon AS ss_icon,
        solscan.twitter AS ss_twitter,
        solscan.website AS ss_website,
        cg.name AS cg_name,
        cg.symbol AS cg_symbol,
        cmc.cmc_name,
        cmc.cmc_symbol,
        cmc.cmc_description,
        cmc.cmc_icon,
        cmc.cmc_tags,
        cmc.cmc_twitter,
        cmc.cmc_urls,
        GREATEST(
            COALESCE(
                solscan._inserted_timestamp,
                '1900-01-01'
            ),
            COALESCE(
                cg._inserted_timestamp,
                '1900-01-01'
            ),
            COALESCE(
                cmc._inserted_timestamp,
                '1900-01-01'
            )
        ) AS _inserted_timestamp
    FROM
        base A
        LEFT JOIN {{ ref('silver__solscan_tokens') }}
        solscan
        ON A.token_address = solscan.token_address
        LEFT JOIN (
            SELECT
                id,
                token_address,
                NAME,
                symbol,
                _inserted_timestamp
            FROM
                {{ source(
                    'crosschain_silver',
                    'asset_metadata_coin_gecko'
                ) }}
            WHERE
                platform = 'solana' qualify(ROW_NUMBER() over(PARTITION BY token_address
            ORDER BY
                _inserted_timestamp DESC) = 1)
        ) cg
        ON A.token_address = cg.token_address
        LEFT JOIN cmc_base cmc
        ON A.token_address = cmc.token_address
)
SELECT
    token_address,
    COALESCE(
        cg_name,
        ss_name,
        cmc_name
    ) AS token_name,
    COALESCE(
        cg_symbol,
        ss_symbol,
        cmc_symbol
    ) AS symbol,
    ss_decimals decimals,
    coin_gecko_id,
    coin_market_cap_id,
    COALESCE(
        CASE
            WHEN COALESCE(
                ss_tags,
                'null'
            ) <> 'null'
            AND COALESCE(
                cmc_tags,
                'null'
            ) <> 'null' THEN ARRAY_CAT(
                ss_tags,
                cmc_tags
            )
        END,
        ss_tags,
        cmc_tags
    ) AS tags,
    COALESCE(
        ss_icon,
        cmc_icon
    ) AS logo,
    COALESCE(
        ss_twitter,
        cmc_twitter
    ) AS twitter,
    COALESCE(
        ss_website,
        cmc_urls
    ) AS website,
    cmc_description AS description,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['token_address']
    ) }} AS labels_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin
