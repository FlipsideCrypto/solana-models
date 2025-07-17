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

{% enddocs %} 