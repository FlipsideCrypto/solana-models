{% docs fact_token_balances %}

## Description
This table contains one row per account and transaction where an SPL token balance changes on the Solana blockchain. It tracks pre- and post-transaction balances for each token account, including block, transaction, mint, and owner information. Balances are decimal adjusted according to the token's mint. Each row represents a unique token account-transaction balance change event, enabling analysis of token balance changes, account activity, and ownership attribution at the transaction level.

## Key Use Cases
- Analyze token balance changes and account activity
- Attribute token balances and transfers to the correct owner at any point in time
- Support analytics on token flows, DeFi protocol activity, and wallet histories
- Study token distribution, holder analysis, and whale tracking
- Enable time-series and event-based analytics on token balances

## Important Relationships
- Closely related to `core.fact_token_account_owners` (for historical ownership), `core.fact_events` (for event context), and `core.ez_transfers` (for transfer events)
- Use `core.fact_token_account_owners` to attribute balances to the correct owner
- Use `core.fact_events` for event-level context and protocol interactions
- Use `core.ez_transfers` for asset movement and transfer analytics
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and balance change analysis
- `block_id`, `tx_id`, `tx_index`: For unique identification and joins
- `account_address`, `mint`, `owner`: For account, token, and ownership analytics
- `pre_balance`, `balance`: For before/after balance analysis

{% enddocs %} 