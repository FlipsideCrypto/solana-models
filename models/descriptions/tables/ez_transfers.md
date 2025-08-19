{% docs ez_transfers %}

## Description
This table contains transfer events for Solana and SPL tokens, including pre-parsed transfer amounts, USD value, token symbol, and verification status. Each row represents a single transfer event with enriched metadata that makes analytics easier compared to the raw `core.fact_transfers` table. The model includes both SOL and token transfers with complete pricing and token metadata, supporting comprehensive analytics on asset movement, wallet activity, payment flows, and large value transfers.

**This is the preferred table for transfer analytics** - it includes USD pricing, token symbols, verification status, and other enriched fields that are essential for most analytical use cases.

## Key Use Cases
- Track SOL and SPL token movements with USD valuations
- Analyze payment flows and wallet transaction histories
- Monitor large value transfers and whale activity with price context
- Build DeFi protocol volume analytics with accurate USD measurements
- Analyze token adoption and usage patterns using verified token metadata
- Create comprehensive asset movement dashboards with pricing data

## Important Relationships
- Enhanced version of `core.fact_transfers` with additional pricing and metadata fields
- Closely related to `core.fact_events` (for event context), `core.fact_events_inner` (for inner/CPI events), and `core.ez_events_decoded` (for decoded instruction details)
- Uses token price data from price tables to compute USD values
- Joins with `core.fact_transactions` for transaction context
- Essential for DeFi analytics when combined with protocol-specific tables like `defi.ez_dex_swaps` and `defi.ez_liquidity_pool_actions`

## Commonly-used Fields
- `block_timestamp`: For time-series and transfer sequencing analysis
- `tx_id`, `block_id`: For transaction and block context joins
- `tx_from`, `tx_to`: For sender and recipient analysis and wallet tracking
- `amount`, `amount_usd`: For value analysis in both token units and USD
- `mint`, `symbol`: For token-specific analytics and filtering
- `token_is_verified`: For filtering to verified/trusted tokens
- `signer`: For transaction initiator analysis

{% enddocs %}
