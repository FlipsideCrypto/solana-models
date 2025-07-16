{% docs ez_signers %}

## Description
This table contains one row per unique Solana signer (address that signs transactions), with activity metrics, first/last transaction dates, programs used, and fee totals. Each row represents a unique signer and their activity profile, supporting analytics on user and protocol adoption, signer activity, and fee contributions across the Solana blockchain.

## Key Use Cases
- Analyze signer activity and protocol adoption
- Track user and protocol engagement over time
- Study fee contributions and transaction patterns
- Segment signers by activity, protocol, and time
- Support analytics on user growth, retention, and protocol usage

## Important Relationships
- Closely related to `core.fact_transactions` (for transaction context), `core.fact_events` (for event context), and `core.fact_transfers` (for transfer events)
- Use `core.fact_transactions` to analyze transaction-level activity
- Use `core.fact_events` for event-level context and protocol interactions
- Use `core.fact_transfers` for asset movement and transfer analytics
- Joins with `core.fact_blocks` for block context

## Commonly-used Fields
- `signer`: For user and protocol identification
- `first_tx_date`, `last_tx_date`: For activity period analysis
- `num_days_active`, `num_txs`: For engagement and usage analytics
- `total_fees`: For fee contribution analysis
- `programs_used`: For protocol usage segmentation

{% enddocs %} 