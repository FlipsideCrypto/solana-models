{% docs fact_bridge_activity %}

## Description
This table contains bridging actions across major cross-chain protocols including Wormhole, DeBridge, and Mayan Finance. It tracks token transfers between Solana and other blockchains, capturing both inbound (to Solana) and outbound (from Solana) bridge activity. Each row represents a single bridge transaction, supporting analytics on cross-chain liquidity flows, bridge protocol usage, and multi-chain DeFi activity.

## Key Use Cases
- Analyze cross-chain liquidity flows and bridge protocol adoption
- Track token movements between Solana and other blockchains
- Study bridge protocol usage patterns and user behavior
- Monitor cross-chain DeFi activity and capital flows
- Support analytics on multi-chain ecosystem growth and integration

## Important Relationships
- Closely related to `defi.ez_bridge_activity` (preferred for most analytics), `defi.ez_liquidity_pool_actions` (for liquidity flows), and `defi.ez_dex_swaps` (for DEX activity)
- Use `defi.ez_bridge_activity` for enhanced bridge analytics with USD pricing and token metadata
- Use `defi.ez_liquidity_pool_actions` to analyze liquidity provision after bridging
- Use `defi.ez_dex_swaps` to track trading activity of bridged tokens
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and bridge flow analysis
- `direction`: For filtering inbound vs outbound transfers
- `platform`: For protocol-specific analysis
- `user_address`, `amount`, `mint`: For user and token analytics
- `succeeded`: For transaction success analysis

{% enddocs %} 