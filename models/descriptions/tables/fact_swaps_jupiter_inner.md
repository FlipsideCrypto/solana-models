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
- `block_timestamp`, `swap_index`, `swap_from_mint`, `swap_to_mint`, `swap_from_amount`, `swap_to_amount`, `program_id`

{% enddocs %} 