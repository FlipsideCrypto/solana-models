{% docs fact_swaps_jupiter_summary %}

## Description
This table contains summary information for Jupiter aggregator swaps, representing the complete user transaction rather than individual route steps. It captures the overall swap experience including total amounts, user addresses, and special features like DCA (Dollar Cost Averaging) and limit orders. Each row represents a single Jupiter swap transaction, supporting analytics on Jupiter usage, user behavior, and aggregator performance. Summarizes Jupiter swap data without the intermediate routes that Jupiter performs 'behind the scenes.'

## Key Use Cases
- Analyze the intended swap activity (initial in and final out amount)
- Information on user-initiated swaps on Jupiter
- Compare swap activity between DEXes at the start and end of the route

## Important Relationships
- Use with `defi.fact_swaps_jupiter_inner` for full route details
- Use with `defi.fact_swaps` and `defi.ez_dex_swaps` for comparison with direct swaps
- Example queries: number of buy and sell swaps for a token on Jupiter, compare routed vs. direct swap volume

## Commonly-used Fields
- `block_timestamp`, `swapper`, `swap_from_mint`, `swap_to_mint`, `swap_from_amount`, `swap_to_amount`, `is_dca_swap`, `is_limit_swap`

## Sample Queries

### User-initiated buy/sell analysis for Jupiter swaps
```sql
-- Number of buy and sell swaps for JTO token on Jupiter
WITH sells AS (
    SELECT 
        block_timestamp::date AS dt,
        SUM(swap_from_amount) AS sum_sells,
        COUNT(DISTINCT swapper) AS unique_sellers
    FROM solana.defi.fact_swaps_jupiter_summary
    WHERE swap_from_mint = 'jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL'
        AND block_timestamp::date BETWEEN '2025-01-01' AND '2025-01-07'
    GROUP BY 1
),
buys AS (
    SELECT 
        block_timestamp::date AS dt,
        SUM(swap_to_amount) AS sum_buys,
        COUNT(DISTINCT swapper) AS unique_buyers
    FROM solana.defi.fact_swaps_jupiter_summary
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

### Jupiter DCA and limit order analysis
```sql
-- Analyze special Jupiter features usage
SELECT 
    block_timestamp::date AS dt,
    COUNT(*) AS total_jupiter_swaps,
    COUNT(CASE WHEN is_dca_swap THEN 1 END) AS dca_swaps,
    COUNT(CASE WHEN is_limit_swap THEN 1 END) AS limit_swaps,
    COUNT(CASE WHEN NOT is_dca_swap AND NOT is_limit_swap THEN 1 END) AS regular_swaps,
    COUNT(CASE WHEN is_dca_swap THEN 1 END) * 100.0 / COUNT(*) AS dca_percentage,
    COUNT(CASE WHEN is_limit_swap THEN 1 END) * 100.0 / COUNT(*) AS limit_percentage
FROM solana.defi.fact_swaps_jupiter_summary
WHERE block_timestamp >= CURRENT_DATE - 30
GROUP BY 1
ORDER BY dt;
```

### Jupiter user behavior and intent analysis
```sql
-- Analyze intended swap activity and user patterns on Jupiter
SELECT 
    COUNT(DISTINCT swapper) AS unique_users,
    COUNT(*) AS total_swaps,
    AVG(swap_from_amount) AS avg_input_amount,
    AVG(swap_to_amount) AS avg_output_amount,
    COUNT(*) / COUNT(DISTINCT swapper) AS avg_swaps_per_user,
    COUNT(DISTINCT swap_from_mint) AS unique_input_tokens,
    COUNT(DISTINCT swap_to_mint) AS unique_output_tokens
FROM solana.defi.fact_swaps_jupiter_summary
WHERE block_timestamp >= CURRENT_DATE - 7
    AND succeeded = true;
```

### Compare Jupiter vs all DEX activity
```sql
-- Compare Jupiter summary volume vs total DEX volume
WITH jupiter_volume AS (
    SELECT 
        block_timestamp::date AS dt,
        SUM(swap_from_amount) AS jupiter_volume,
        COUNT(*) AS jupiter_swaps,
        COUNT(DISTINCT swapper) AS jupiter_users
    FROM solana.defi.fact_swaps_jupiter_summary
    WHERE block_timestamp::date BETWEEN '2025-01-01' AND '2025-01-07'
    GROUP BY 1
),
total_dex_volume AS (
    SELECT 
        block_timestamp::date AS dt,
        SUM(swap_from_amount) AS total_dex_volume,
        COUNT(*) AS total_swaps,
        COUNT(DISTINCT swapper) AS total_users
    FROM solana.defi.fact_swaps
    WHERE block_timestamp::date BETWEEN '2025-01-01' AND '2025-01-07'
    GROUP BY 1
)
SELECT 
    j.dt,
    j.jupiter_volume,
    t.total_dex_volume,
    j.jupiter_volume * 100.0 / t.total_dex_volume AS jupiter_market_share_pct,
    j.jupiter_swaps,
    t.total_swaps,
    j.jupiter_users,
    t.total_users
FROM jupiter_volume j
JOIN total_dex_volume t ON j.dt = t.dt
ORDER BY j.dt;
```

{% enddocs %} 