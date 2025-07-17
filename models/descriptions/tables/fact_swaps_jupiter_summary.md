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

{% enddocs %} 