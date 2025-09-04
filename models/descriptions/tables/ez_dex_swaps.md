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

### Large swaps by size and impact
```sql
-- Large swaps by size and impact
SELECT 
    block_timestamp,
    tx_id,
    swap_program,
    swapper,
    swap_from_symbol || ' -> ' || swap_to_symbol AS swap_pair,
    swap_from_amount,
    swap_from_amount_usd,
    swap_to_amount,
    swap_to_amount_usd,
    ABS(swap_from_amount_usd - swap_to_amount_usd) / NULLIF(swap_from_amount_usd, 0) * 100 AS slippage_pct
FROM solana.defi.ez_dex_swaps
WHERE block_timestamp >= CURRENT_DATE - 1
    AND swap_from_amount_usd > 100000  -- Swaps over $100k
ORDER BY swap_from_amount_usd DESC
LIMIT 100;
```

### Platform market share by volume
```sql
-- Platform market share by volume
WITH platform_stats AS (
    SELECT 
        swap_program,
        SUM(swap_from_amount_usd) AS total_volume,
        COUNT(*) AS total_swaps,
        COUNT(DISTINCT swapper) AS unique_users,
        COUNT(DISTINCT program_id) AS unique_programs
    FROM solana.defi.ez_dex_swaps
    WHERE block_timestamp >= CURRENT_DATE - 7
        AND swap_from_amount_usd IS NOT NULL
    GROUP BY 1
)
SELECT 
    swap_program,
    total_volume,
    ROUND(100.0 * total_volume / SUM(total_volume) OVER (), 2) AS market_share_pct,
    total_swaps,
    unique_users,
    unique_programs,
    total_volume / NULLIF(total_swaps, 0) AS avg_swap_size
FROM platform_stats
ORDER BY total_volume DESC;
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

### DEX performance comparison
```sql
-- Compare DEX efficiency and user activity
SELECT 
    swap_program,
    COUNT(*) AS total_swaps,
    COUNT(DISTINCT swapper) AS unique_users,
    SUM(swap_from_amount_usd) AS volume_usd,
    AVG(swap_from_amount_usd) AS avg_swap_size,
    COUNT(*) / COUNT(DISTINCT swapper) AS swaps_per_user,
    SUM(CASE WHEN swap_from_is_verified AND swap_to_is_verified THEN 1 ELSE 0 END) / COUNT(*) * 100 AS verified_token_pct
FROM solana.defi.ez_dex_swaps
WHERE block_timestamp >= CURRENT_DATE - 7
    AND swap_from_amount_usd IS NOT NULL
GROUP BY 1
ORDER BY volume_usd DESC;
```

{% enddocs %} 