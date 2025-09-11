{% docs fact_swaps_jupiter_inner %}

## Description
This table contains each intermediate swap that is part of a Jupiter route, representing the individual steps executed by Jupiter to complete a swap. It provides granular visibility into the routing logic and execution path of Jupiter aggregator swaps, capturing the specific DEXes and pools used for each step. Each row represents a single intermediate swap within a larger Jupiter transaction, supporting detailed analysis of Jupiter's routing efficiency and DEX usage patterns.

## Key Use Cases
- Analyze the full path and DEX usage for Jupiter-routed swaps
- Calculate routed swap volume and compare with direct swaps
- Understand Jupiter’s routing logic and total impact on DEX activity

## Important Relationships
- Use with `defi.fact_swaps_jupiter_summary` for user-level intent and summarized swaps
- Use with `defi.fact_swaps` to compare routed and direct swaps
- Example queries: buy/sell volume for a token routed on Jupiter, compare volume of Jupiter-routed swaps vs. direct DEX swaps

## Commonly-used Fields
- `block_timestamp`, `swap_index`, `swap_from_mint`, `swap_to_mint`, `swap_from_amount`, `swap_to_amount`, `swap_program_id`, `aggregator_program_id`

## Sample Queries

### Analyze Jupiter routing for a specific token
```sql
-- Get number of swaps that routed through Jupiter for PYTH token
SELECT
    block_timestamp::date AS dt,
    SUM(swap_from_amount) AS daily_pyth_swapped_from,
    COUNT(*) AS num_intermediate_swaps,
    COUNT(DISTINCT tx_id) AS num_jupiter_transactions,
    COUNT(DISTINCT swap_program_id) AS unique_dexes_used
FROM solana.defi.fact_swaps_jupiter_inner 
WHERE swap_from_mint = 'HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3'
    AND block_timestamp::date BETWEEN '2025-01-01' AND '2025-01-07'
GROUP BY 1
ORDER BY dt;
```

### Jupiter DEX usage and routing analysis
```sql
-- Analyze which DEXes Jupiter routes through most frequently
SELECT 
    l.address_name AS dex_program,
    COUNT(*) AS total_route_steps,
    COUNT(DISTINCT tx_id) AS unique_transactions,
    SUM(swap_from_amount) AS total_volume_routed,
    AVG(swap_from_amount) AS avg_swap_size
FROM solana.defi.fact_swaps_jupiter_inner j
LEFT JOIN solana.core.dim_labels l ON j.swap_program_id = l.address
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1
ORDER BY total_route_steps DESC;
```

### Jupiter routing complexity analysis
```sql
-- Analyze the complexity of Jupiter routes (number of steps per transaction)
SELECT 
    COUNT(DISTINCT swap_index) AS route_steps,
    COUNT(*) AS num_transactions,
    AVG(swap_from_amount) AS avg_initial_amount,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct_of_transactions
FROM solana.defi.fact_swaps_jupiter_inner
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY tx_id, 1
ORDER BY route_steps;
```

{% enddocs %} 