{% docs ez_liquidity_pool_actions %}

## Description
This table captures liquidity pool actions (deposits and withdrawals) across major Solana DeFi protocols including Raydium, Orca, and Meteora. It provides a unified view of liquidity provision and removal events with USD pricing, token verification status, and platform identification. Each row represents a single liquidity pool action, supporting analytics on DeFi protocol usage, liquidity provision patterns, and yield farming activities.

## Key Use Cases
- Analyze liquidity provision and removal patterns across DeFi protocols
- Track yield farming activities and liquidity mining incentives
- Study protocol adoption and user behavior in liquidity pools
- Monitor USD-denominated liquidity flows and TVL changes
- Support analytics on DeFi protocol performance and user engagement

## Important Relationships
- Closely related to `defi.fact_swaps` (for swap activity), `defi.ez_dex_swaps` (for DEX activity), and `defi.fact_bridge_activity` (for cross-chain flows)
- Use `defi.fact_swaps` and `defi.ez_dex_swaps` to analyze trading activity in liquidity pools
- Use `defi.fact_bridge_activity` to understand cross-chain liquidity flows
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and liquidity flow analysis
- `action_type`: For filtering deposits vs withdrawals
- `provider_address`, `pool_address`: For user and pool identification
- `token_a_mint`, `token_b_mint`, `token_a_amount_usd`, `token_b_amount_usd`: For token and value analytics
- `platform`: For protocol-specific analysis

{% enddocs %} 