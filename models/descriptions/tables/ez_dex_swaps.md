{% docs ez_dex_swaps %}

## Description
This table provides an enhanced view of DEX swap activity with USD pricing, token verification status, and enriched metadata. It aggregates swap data from `defi.fact_swaps` and enriches it with price information and token details, making it the preferred table for most DEX analytics. Includes swaps from curated DEX protocols and standalone swaps, supporting detailed analysis of trading activity, value flows, and market dynamics.

## Key Use Cases
- Analyze DEX trading activity with USD-denominated metrics
- Calculate real swap volume and compare swap activity across DEXes
- Compare activity across different curated swap programs
- Analyze user intent and distinguish routed vs. direct swaps

## Important Relationships
- Source data from `defi.fact_swaps`
- Use with `defi.fact_swaps_jupiter_inner` and `defi.fact_swaps_jupiter_summary` for routed swap analysis
- Example queries: swap metrics by DEX protocol, compare activity across routed/direct swaps

## Commonly-used Fields
- `block_timestamp`, `swapper`, `swap_from_mint`, `swap_to_mint`, `swap_from_amount_usd`, `swap_to_amount_usd`, `swap_program`

## Sample Queries

### Real swap volume calculation by DEX protocol
```sql
-- Calculate real swap volume by DEX protocol (includes both direct and Jupiter-routed swaps)
SELECT 
    swap_program,
    COUNT(*) AS total_swaps,
    COUNT(DISTINCT swapper) AS unique_users,
    SUM(swap_from_amount_usd) AS total_volume_usd,
    AVG(swap_from_amount_usd) AS avg_swap_size_usd
FROM solana.defi.ez_dex_swaps
WHERE block_timestamp >= CURRENT_DATE - 7
    AND swap_from_amount_usd IS NOT NULL
GROUP BY 1
ORDER BY total_volume_usd DESC;
```

### Buy/sell volume analysis for a specific token
```sql
-- Get buy/sell volume for JTO token across all DEXes
WITH sells AS (
    SELECT 
        block_timestamp::date AS dt,
        SUM(swap_from_amount) AS sum_sells,
        COUNT(DISTINCT swapper) AS unique_sellers
    FROM solana.defi.ez_dex_swaps
    WHERE swap_from_mint = 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL'
        AND block_timestamp::date BETWEEN '2025-01-01' AND '2025-01-07'
    GROUP BY 1
),
buys AS (
    SELECT 
        block_timestamp::date AS dt,
        SUM(swap_to_amount) AS sum_buys,
        COUNT(DISTINCT swapper) AS unique_buyers
    FROM solana.defi.ez_dex_swaps
    WHERE swap_to_mint = 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL'
        AND block_timestamp::date BETWEEN '2025-01-01' AND '2025-01-07'
    GROUP BY 1
)
SELECT 
    COALESCE(s.dt, b.dt) AS dt,
    COALESCE(s.sum_sells, 0) AS sell_volume,
    COALESCE(s.unique_sellers, 0) AS unique_sellers,
    COALESCE(b.sum_buys, 0) AS buy_volume,
    COALESCE(b.unique_buyers, 0) AS unique_buyers
FROM sells s
FULL OUTER JOIN buys b ON s.dt = b.dt
ORDER BY dt;
```

### Token pair trading analysis
```sql
-- Most traded token pairs by volume
SELECT 
    swap_from_symbol || ' -> ' || swap_to_symbol AS trading_pair,
    COUNT(*) AS swap_count,
    SUM(swap_from_amount_usd) AS total_volume_usd,
    AVG(swap_from_amount_usd) AS avg_swap_size,
    COUNT(DISTINCT swapper) AS unique_traders,
    COUNT(DISTINCT swap_program) AS dex_count
FROM solana.defi.ez_dex_swaps
WHERE block_timestamp >= CURRENT_DATE - 7
    AND swap_from_amount_usd > 0
    AND swap_from_symbol IS NOT NULL 
    AND swap_to_symbol IS NOT NULL
GROUP BY 1
HAVING swap_count >= 10
ORDER BY total_volume_usd DESC
LIMIT 50;
```

### Compare Jupiter-routed vs direct DEX swaps
```sql
-- Compare volume of Jupiter-routed swaps vs direct DEX swaps for wSOL
with jupiter_routed_swaps as (
select 
tx_id,
block_timestamp, 
swap_from_amount 
from solana.defi.fact_swaps_jupiter_inner
where swap_from_mint = 'So11111111111111111111111111111111111111112'
and block_timestamp::date between '2025-05-01' and '2025-05-07')
,
 
direct_swaps as (
select 
tx_id, 
block_timestamp, 
swap_from_amount 
from solana.defi.fact_swaps
where swap_from_mint = 'So11111111111111111111111111111111111111112'
and block_timestamp::date between '2025-05-01' and '2025-05-07'
and tx_id not in (select distinct(tx_id) from jupiter_routed_swaps) -- need to exclude the jupiter related swaps from fact_swaps
)
,
 
sum_jupiter as (
select 
block_timestamp::date dt, 
sum(swap_from_amount) as jup_amt
from jupiter_routed_swaps
group by 1)
 
,
sum_direct as (
select 
block_timestamp::date dt, 
sum(swap_from_amount) as direct_amt
from direct_swaps
group by 1)
 
Select
a.dt, 
a.jup_amt as swap_via_jupiter,
b.direct_amt as direct_swap, 
a.jup_amt + b.direct_amt as total_amt
from sum_jupiter a
left join sum_direct b
on a.dt = b.dt;
```

{% enddocs %} 