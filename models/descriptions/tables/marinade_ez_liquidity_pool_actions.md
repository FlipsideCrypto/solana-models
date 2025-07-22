{% docs marinade_ez_liquidity_pool_actions %}

## Description
This table captures actions for liquidity pools that use either MSOL or MNDE as one of the liquidity pairs, including deposit and withdrawal events. It provides a unified view of liquidity provision activities across multiple DeFi protocols with USD pricing information, enabling comprehensive analysis of liquidity pool participation and token pair dynamics specifically for Marinade Finance ecosystem.

## Key Use Cases
- Analyze liquidity provision patterns for MSOL and MNDE pools
- Track liquidity pool deposits and withdrawals across protocols
- Monitor liquidity depth and availability for Marinade tokens
- Study liquidity provider behavior and preferences
- Support liquidity pool performance and risk analysis

## Important Relationships
- Links to `marinade.dim_pools` for pool metadata and categorization
- Connects to various `silver.liquidity_pool_actions_*` tables for platform-specific data
- References `price.ez_prices_hourly` for USD price conversion
- Provides liquidity context for Marinade Finance ecosystem analytics

## Commonly-used Fields
- `block_timestamp`: Essential for time-series analysis and trend detection
- `action_type`: Critical for categorizing deposit vs withdrawal activities
- `provider_address`: Key for liquidity provider analysis and behavior tracking
- `pool_address`: Important for pool-specific analysis and filtering
- `token_a_amount_usd` and `token_b_amount_usd`: Critical for value analysis and financial metrics
- `platform`: Key for cross-protocol comparison and platform-specific analysis

{% enddocs %} 