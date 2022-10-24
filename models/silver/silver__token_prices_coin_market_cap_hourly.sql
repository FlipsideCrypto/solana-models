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
                    'hourly_prices_coin_market_cap'
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
            'asset_metadata_coin_market_cap'
        ) }}
    WHERE
        _inserted_timestamp = (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ source(
                    'crosschain_silver',
                    'asset_metadata_coin_market_cap'
                ) }}
        )
        AND id IN (
            5426,
            8526,
            11461,
            9549,
            12297,
            9015,
            7978,
            12236,
            11171,
            13524,
            6187,
            3408,
            825,
            7129,
            4195,
            11181
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
        asset_id :: NUMBER AS id,
        symbol,
        price AS CLOSE
    FROM
        {{ source(
            'shared',
            'prices_v2'
        ) }}
    WHERE
        provider = 'coinmarketcap'
        AND asset_id IN (
            '5426',
            '8526',
            '11461',
            '9549',
            '12297',
            '9015',
            '7978',
            '12236',
            '11171',
            '13524',
            '6187',
            '3408',
            '825',
            '7129',
            '4195',
            '11181'
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
            'hourly_prices_coin_market_cap'
        ) }}
        p
        LEFT OUTER JOIN asset_metadata m
        ON m.id = p.id
    WHERE
        p.id IN (
            5426,
            8526,
            11461,
            9549,
            12297,
            9015,
            7978,
            12236,
            11171,
            13524,
            6187,
            3408,
            825,
            7129,
            4195,
            11181
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
