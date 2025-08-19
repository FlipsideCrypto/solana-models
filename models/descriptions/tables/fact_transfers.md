{% docs fact_transfers %}

## Description
This table contains one row per transfer event involving native SOL or SPL tokens on the Solana blockchain. Each row records the details of a single transfer, including block and transaction identifiers, sender and recipient addresses, transferred amount, and token mint. The model includes both SOL and token transfers, supporting analytics on asset movement, user activity, and protocol flows. Data is updated as new blocks are processed, and each row represents a unique transfer event within a transaction.

**Note: For most transfer analytics, prefer using `core.ez_transfers` which includes additional enriched fields like USD values, token symbols, and verification status that make analytics easier.**

## Key Use Cases
- Track SOL or SPL token movements between accounts
- Analyze payment flows and wallet transaction histories
- Monitor large value transfers and whale activity
- Build DeFi protocol volume and token flow analytics
- Simplify asset movement analysis compared to parsing raw instructions

## Important Relationships
- **Prefer `core.ez_transfers` for most transfer analytics** - enriched version with USD values, token symbols, and verification status
- Closely related to `core.fact_events` (for event context), `core.fact_events_inner` (for inner/CPI events), and `core.ez_events_decoded` (preferred for decoded instruction details)
- Use `core.fact_events` for event-level context and protocol interactions
- Use `core.fact_events_inner` for nested program calls and composability analysis
- Use `core.ez_events_decoded` for detailed instruction and argument analysis (if program is being decoded)
- Joins with `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and transfer sequencing analysis
- `block_id`, `tx_id`, `index`: For unique transfer identification and joins
- `tx_from`, `tx_to`: For sender and recipient analysis
- `amount`, `mint`: For value and token-specific analytics

{% enddocs %} 