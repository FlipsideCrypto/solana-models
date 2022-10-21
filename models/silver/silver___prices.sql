{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', date_hour, id)",
    incremental_strategy = 'merge',
    cluster_by = ['date_hour::DATE'],
) }}
with date_hours as (
    select date_hour
    from crosschain_dev.core.dim_date_hours
    where date_hour >= '2020-04-10'
),
asset_metadata as (
    select 
        id, 
        symbol
    from crosschain.silver.asset_metadata_coin_market_cap
    where _inserted_timestamp = (select max(_inserted_timestamp) from crosschain.silver.asset_metadata_coin_market_cap)
    and id in (5426,8526,11461,9549,12297,9015,7978,12236,11171,13524,6187,3408,825,7129,4195,11181)
    group by 1,2
),
base_date_hours_symbols as (
    select 
        date_hour,
        symbol
    from date_hours
    cross join (select symbol from asset_metadata)
),
base_legacy_prices as (
    SELECT
        date_trunc('hour',recorded_at) as recorded_hour,
        symbol,
        price as close
    FROM
        flipside_prod_db.silver.prices_v2
    WHERE
        provider = 'coinmarketcap'
        AND asset_id in ('5426','8526','11461','9549','12297','9015','7978','12236','11171','13524','6187','3408','825','7129','4195','11181')
        AND minute(recorded_at) = 59
        AND recorded_at::date < '2022-07-20'
),
base_prices as (
    select recorded_hour, m.symbol, p.close
    from crosschain.silver.hourly_prices_coin_market_cap p 
    left outer join asset_metadata m on m.id = p.id
    where p.id in (5426,8526,11461,9549,12297,9015,7978,12236,11171,13524,6187,3408,825,7129,4195,11181)
),
prices as (
    select *
    from base_legacy_prices
    union 
    select *
    from base_prices
),
imputed_prices as (
    select
        d.*,
        p.close,
        last_value(p.close ignore nulls) over (partition by d.symbol order by d.date_hour rows unbounded preceding) as imputed_close
    from base_date_hours_symbols d 
    left outer join prices p on p.recorded_hour = d.date_hour and p.symbol = d.symbol
)
select 
    p.date_hour,
    p.symbol,
    coalesce(p.close,p.imputed_close) as close2,
    case when p.close is null then true else false end as imputed
from imputed_prices p
where close2 is not null