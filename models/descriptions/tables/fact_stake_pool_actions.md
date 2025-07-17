{% docs fact_stake_pool_actions %}

## Description
This table contains deposit and withdrawal actions with stake pools on the Solana blockchain, including major protocols like Lido, Marinade, and Jito. It tracks liquid staking activities, capturing user interactions with stake pools, staking amounts, and pool-specific metadata. Each row represents a single stake pool action, supporting analytics on liquid staking adoption, yield generation, and staking protocol usage.

## Key Use Cases
- Analyze liquid staking adoption and user behavior
- Track stake pool performance and yield generation
- Study staking protocol usage patterns and preferences
- Monitor staking flows and capital allocation
- Support analytics on DeFi staking ecosystem growth

## Important Relationships
- Closely related to `defi.ez_liquidity_pool_actions` (for liquidity provision), `defi.ez_dex_swaps` (for DEX activity), and `core.fact_transfers` (for token movements)
- Use `defi.ez_liquidity_pool_actions` to analyze liquidity provision in staking-related pools
- Use `defi.ez_dex_swaps` to track trading of liquid staking tokens
- Use `core.fact_transfers` to analyze token movements related to staking
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and staking activity analysis
- `stake_pool_name`, `action`: For pool and action identification
- `amount`: For staking amount analytics
- `address`: For user and pool address analysis
- `succeeded`: For transaction success analysis

{% enddocs %} 