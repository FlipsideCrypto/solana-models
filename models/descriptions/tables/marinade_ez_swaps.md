{% docs marinade_ez_swaps %}

## Description
This table contains swaps involving MSOL and MNDE tokens across multiple decentralized exchanges and aggregators. It provides a unified view of trading activities for Marinade Finance tokens with USD pricing information, enabling comprehensive analysis of token liquidity and trading patterns.

## Key Use Cases
- Analyze MSOL and MNDE trading volume and patterns
- Track token liquidity across different DEX platforms
- Monitor trading activity and price impact
- Study user trading behavior and preferences
- Support token economics and market analysis

## Important Relationships
- Links to `defi.fact_swaps_jupiter_summary` for Jupiter aggregator swaps
- Connects to `silver.marinade_swaps` for direct DEX swaps
- References `price.ez_prices_hourly` for USD price conversion
- Connects to `core.dim_labels` for platform identification
- Provides trading context for Marinade Finance ecosystem analytics

## Commonly-used Fields
- `block_timestamp`: Essential for time-series analysis and trend detection
- `swapper`: Key for user analysis and trading behavior tracking
- `swap_from_mint` and `swap_to_mint`: Critical for token pair analysis and filtering
- `swap_from_amount_usd` and `swap_to_amount_usd`: Important for value analysis and financial metrics
- `platform`: Key for cross-platform comparison and DEX analysis

{% enddocs %} 