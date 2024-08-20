{{
    config(
        materialized="table"
    )
}}

WITH token_whitelist AS (
    SELECT
        *
    FROM
        {{ ref('seed__meme_tokens') }}
),
tokens as (
    select 
        token_address as mint,
        symbol
    from 
        {{ ref('price__dim_asset_metadata') }}
    inner join 
        token_whitelist
        USING(token_address)
), 
pc0 as (
    select 
        token_address,
        hour,
        price,
        lag(price, 1) over (
            partition by token_address
            order by hour
        ) as previous_price,
        price / previous_price as ratio
    from 
        {{ ref('price__ez_prices_hourly') }} p
    where 
        hour >= '2022-12-25'
        and is_imputed = false
), 
pc1 as (
    select 
        hour::date as date,
        token_address
    from 
        pc0
    where 
        (
            ratio >= 10
            or ratio <= 0.1
        )
), 
p0 as (
    select 
        p.token_address as mint
        , DATE_TRUNC('hour', p.hour) as hour
        , avg(price) as price
        , min(price) as min_price
    from 
        {{ ref('price__ez_prices_hourly') }} p
    left join 
        pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.hour::date
    where 
        hour >= '2022-12-25'
        and pc1.date is null
        and is_imputed = FALSE
        and price < 1000000
    GROUP BY 1, 2
), 
p1 as (
    select 
        p.token_address as mint,
        DATE_TRUNC('day', hour) as date,
        avg(price) as price,
        min(price) as min_price,
    from 
        {{ ref('price__ez_prices_hourly') }} p
    left join 
        pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.hour::date
    where 
        hour >= '2022-12-25'
        and pc1.date is null
        and is_imputed = FALSE
        and price < 1000000
    GROUP BY 1, 2
),
p2 as (
    select 
        p.token_address as mint,
        DATE_TRUNC('week', hour) as week,
        avg(price) as price,
        MIN(price) as min_price
    from 
        {{ ref('price__ez_prices_hourly') }} p
    left join 
        pc1
        on pc1.token_address = p.token_address
        and pc1.date = p.hour::date
    where 
        hour >= '2022-12-25'
        and pc1.date is null
        and is_imputed = FALSE
        and price < 1000000
    GROUP BY 1, 2
),
cur_price as (
    select 
        mint,
        price as cur_price
    from 
        p0
    qualify(
        row_number() over (partition by mint order by hour desc) = 1
    )
),
exclude as (
    select distinct 
        s.tx_id,
        s.block_timestamp
    from 
        solana.defi.fact_swaps s
    where 
        s.block_timestamp >= '2024-03-30'
        and swap_program ilike 'jup%'
)
, t0 as (
    select s.tx_id
    , swapper
    , s.block_timestamp
    , swap_from_mint
    , swap_to_mint
    , swap_from_amount
    , swap_to_amount
    from solana.defi.fact_swaps s
    left join exclude e
        on e.block_timestamp = s.block_timestamp
        and e.tx_id = s.tx_id
    where s.block_timestamp >= '2024-03-30'
        and succeeded
        and not swap_program ilike 'jup%'
        and e.tx_id is null
        and (
                swap_to_mint in (select token_address from token_whitelist)
                or swap_from_mint in (select token_address from token_whitelist)
            )
    union
    select s.tx_id
    , swapper
    , s.block_timestamp
    , swap_from_mint
    , swap_to_mint
    , swap_from_amount
    , swap_to_amount
    from solana.defi.fact_swaps_jupiter_inner s
    where s.block_timestamp >= '2024-03-30'
        and succeeded
        and (
            swap_to_mint in (select token_address from token_whitelist)
            or swap_from_mint in (select token_address from token_whitelist)
        )
),
t0b as (
    select distinct t0.*
    , coalesce(p0f.price, p1f.price, p2f.price) as f_price
    , coalesce(p0t.price, p1t.price, p2t.price) as t_price
    , swap_from_amount * f_price as f_usd
    , swap_to_amount * t_price as t_usd
    , case when swap_to_mint in (
        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
        , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        , 'So11111111111111111111111111111111111111112'
    ) then t_usd when swap_from_mint in (
        'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB'
        , 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
        , 'So11111111111111111111111111111111111111112'
    ) then f_usd else least(f_usd, t_usd) end as usd_value
    from t0
    left join p0 p0f
        on left(p0f.mint, 16) = left(t0.swap_from_mint, 16)
        and p0f.hour = date_trunc('hour', t0.block_timestamp)
    left join p1 p1f
        on left(p1f.mint, 16) = left(t0.swap_from_mint, 16)
        and p1f.date = date_trunc('day', t0.block_timestamp)
    left join p2 p2f
        on left(p2f.mint, 16) = left(t0.swap_from_mint, 16)
        and p2f.week = date_trunc('week', t0.block_timestamp)
    left join p0 p0t
        on left(p0t.mint, 16) = left(t0.swap_to_mint, 16)
        and p0t.hour = date_trunc('hour', t0.block_timestamp)
    left join p1 p1t
        on left(p1t.mint, 16) = left(t0.swap_to_mint, 16)
        and p1t.date = date_trunc('day', t0.block_timestamp)
    left join p2 p2t
        on left(p2t.mint, 16) = left(t0.swap_to_mint, 16)
        and p2t.week = date_trunc('week', t0.block_timestamp)
)
-- select * from t0b
-- amount bought, avg purchase price, amount sold, avg sale price, amount held, current price, net profits
, t1 as (
    select swapper
    , block_timestamp
    , swap_from_mint as mint
    , swap_from_amount as amount
    , 0 as n_buys
    , 1 as n_sales
    , 0 as amount_bought
    , swap_from_amount as amount_sold
    , -swap_from_amount as net_amount
    , 0 as usd_bought
    , usd_value as usd_sold
    from t0b
    union
    select swapper
    , block_timestamp
    , swap_to_mint as mint
    , swap_to_amount as amount
    , 1 as n_buys
    , 0 as n_sales
    , swap_to_amount as amount_bought
    , 0 as amount_sold
    , swap_to_amount as net_amount
    , usd_value as usd_bought
    , 0 as usd_sold
    from t0b
)
-- select * from t1
, t2 as (
    select swapper
    , mint
    , sum(usd_bought) as usd_bought
    , sum(usd_sold) as usd_sold
    , sum(amount_bought) as amount_bought
    , sum(amount_sold) as amount_sold
    , sum(net_amount) as cur_amount
    , sum(n_buys) as n_buys
    , sum(n_sales) as n_sales
    from t1
    group by 1, 2
)
, t3 as (
    select t2.*
    , c.cur_price
    , t2.cur_amount * c.cur_price as usd_remaining
    , usd_sold - usd_bought + usd_remaining as net_usd
    from t2
    join cur_price c
        on c.mint = t2.mint
)
, t4 as (
    select 
    t.symbol
    , t3.*
    , usd_bought / amount_bought as avg_buy_price
    , case when amount_sold = 0 then 0 else usd_sold / amount_sold end as avg_sell_price
    , greatest(0, cur_amount) as amount_remaining
    , (amount_remaining * cur_price) + (least(amount_bought, amount_sold) * avg_sell_price) - amount_bought * avg_buy_price as profit
    from t3
    join tokens t
        on t.mint = t3.mint
    where amount_bought > 0
)
select 
    swapper
    , mint
    , symbol
    , avg_buy_price
    , avg_sell_price
    , amount_bought
    , amount_remaining
    , profit
from t4
qualify
    row_number() over (partition by swapper, mint order by symbol) = 1