{{ config(
    materialized = 'incremental',
    unique_key = "_unique_key",
    incremental_strategy = 'merge',
    cluster_by = ['recorded_hour::DATE'],
) }}

WITH date_hours AS (

    SELECT
        date_hour
    FROM
        {{ source (
            'crosschain',
            'dim_date_hours'
        ) }}
    WHERE
        date_hour >= '2020-04-10'
        AND date_hour <= (
            SELECT
                MAX(recorded_hour)
            FROM
                {{ source(
                    'crosschain_silver',
                    'hourly_prices_coin_gecko'
                ) }}
        )

{% if is_incremental() %}
AND date_hour > (
    SELECT
        MAX(recorded_hour)
    FROM
        {{ this }}
)
{% endif %}
),
asset_metadata AS (
    SELECT
        id,
        symbol
    FROM
        {{ source(
            'crosschain_silver',
            'asset_metadata_coin_gecko'
        ) }}
    WHERE
        id IN (
            'solana',
            'cope',
            'bonfida',
            'jet',
            'mercurial',
            'mango-markets',
            'msol',
            'raydium',
            'saber',
            'solend',
            'serum',
            'lido-staked-sol',
            'ftx-token',
            'usd-coin',
            'tether',
            'terrausd', 
            'terrausd-wormhole', 
            'green-satoshi-token', 
            'stepn', 
            'weth', 
            'wrapped-bitcoin', 
            'step-finance', 
            'star-atlas', 
            'usdh', 
            'star-atlas-dao', 
            'samoyedcoin', 
            'orca', 
            'dust-protocol', 
            'jpool', 
            'defi-land', 
            'genopets', 
            'chainlink-wormhole', 
            'solfarm', 
            'sushi', 
            'woof-token', 
            'kin', 
            'wbnb', 
            'solpad-finance', 
            'cato', 
            'lido-dao-wormhole', 
            'parrot-usd', 
            'media-network',
            'wrapped-avax', 
            'sunny-aggregator', 
            'audius', 
            'liq-protocol', 
            'genopet-ki', 
            'coin98', 
            'cropperfinance', 
            'boring-protocol', 
            'starlaunch', 
            'solchicks-token', 
            'wmatic', 
            'solanium', 
            'uxd-stablecoin', 
            'aurory', 
            'marinade', 
            'blocksmith-labs-forge', 
            'genesysgo-shadow', 
            'parrot-protocol', 
            'fabric', 
            'metaplex', 
            'only1', 
            'bamboo-coin', 
            'oxygen', 
            'terra-luna-2', 
            'larix', 
            'puff', 
            'solrise-finance', 
            'solape-token', 
            'usd-coin-wormhole-from-ethereum', 
            'hubble', 
            'solice', 
            'socean-staked-sol', 
            'cheems', 
            'realy-metaverse', 
            'zebec-protocol', 
            'rally-solana', 
            'meanfi', 
            'aleph', 
            'port-finance', 
            'aldrin', 
            'rope-token', 
            'maps', 
            'cryowar-token', 
            'grape-2', 
            'tomochain', 
            'oogi', 
            'near', 
            'fantom', 
            'sypool', 
            'frakt-token', 
            'yearn-finance', 
            'uniswap-wormhole', 
            'wrapped-steth', 
            'synthetify-token', 
            'dexlab', 
            'run', 
            'bitspawn', 
            'dragon-war', 
            'basis-markets', 
            'nirvana-prana', 
            'investin', 
            'celo',
            'solcial'
        )
    GROUP BY
        1,
        2
),
base_date_hours_symbols AS (
    SELECT
        date_hour,
        id,
        symbol
    FROM
        date_hours
        CROSS JOIN asset_metadata
),
base_legacy_prices AS (
    SELECT
        DATE_TRUNC(
            'hour',
            recorded_at
        ) AS recorded_hour,
        asset_id AS id,
        symbol,
        price AS CLOSE
    FROM
        {{ source(
            'shared',
            'prices_v2'
        ) }}
    WHERE
        provider = 'coingecko'
        AND asset_id IN (
            'solana',
            'cope',
            'bonfida',
            'jet',
            'mercurial',
            'mango-markets',
            'msol',
            'raydium',
            'saber',
            'solend',
            'serum',
            'lido-staked-sol',
            'ftx-token',
            'usd-coin',
            'tether',
            'terrausd'
        )
        AND MINUTE(recorded_at) = 59
        AND recorded_at :: DATE < '2022-07-20' -- use legacy data before this date

{% if is_incremental() %}
AND recorded_at > (
    SELECT
        MAX(recorded_hour)
    FROM
        {{ this }}
)
{% endif %}
),
base_prices AS (
    SELECT
        recorded_hour,
        p.id,
        m.symbol,
        p.close
    FROM
        {{ source(
            'crosschain_silver',
            'hourly_prices_coin_gecko'
        ) }}
        p
        LEFT OUTER JOIN asset_metadata m
        ON m.id = p.id
    WHERE
        p.id IN (
            'solana',
            'cope',
            'bonfida',
            'jet',
            'mercurial',
            'mango-markets',
            'msol',
            'raydium',
            'saber',
            'solend',
            'serum',
            'lido-staked-sol',
            'ftx-token',
            'usd-coin',
            'tether',
            'terrausd'
        )
        AND recorded_hour :: DATE >= '2022-07-20'

{% if is_incremental() %}
AND recorded_hour > (
    SELECT
        MAX(recorded_hour)
    FROM
        {{ this }}
)
{% endif %}
),
prices AS (
    SELECT
        *
    FROM
        base_legacy_prices
    UNION
    SELECT
        *
    FROM
        base_prices
),
imputed_prices AS (
    SELECT
        d.*,
        p.close AS hourly_close,
        LAST_VALUE(
            p.close ignore nulls
        ) over (
            PARTITION BY d.symbol
            ORDER BY
                d.date_hour rows unbounded preceding
        ) AS imputed_close
    FROM
        base_date_hours_symbols d
        LEFT OUTER JOIN prices p
        ON p.recorded_hour = d.date_hour
        AND p.id = d.id
)
SELECT
    p.date_hour AS recorded_hour,
    p.id,
    p.symbol,
    COALESCE(
        p.hourly_close,
        p.imputed_close
    ) AS CLOSE,
    CASE
        WHEN p.hourly_close IS NULL THEN TRUE
        ELSE FALSE
    END AS imputed,
    concat_ws(
        '-',
        recorded_hour,
        id
    ) AS _unique_key
FROM
    imputed_prices p
WHERE
    CLOSE IS NOT NULL
