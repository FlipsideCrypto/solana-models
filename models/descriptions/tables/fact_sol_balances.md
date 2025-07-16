{% docs fact_sol_balances %}

## Description
This table contains one row per account and transaction where a native SOL (Solana) balance changes on the Solana blockchain. It tracks pre- and post-transaction balances for each account, including block, transaction, mint, and owner information. Balances are adjusted for SOL's 9 decimal places. Each row represents a unique account-transaction balance change event, enabling analysis of SOL balance changes, account activity, and ownership attribution at the transaction level.

## Key Use Cases
- Analyze SOL balance changes and account activity
- Attribute SOL balances and transfers to the correct owner at any point in time
- Support analytics on SOL flows, DeFi protocol activity, and wallet histories
- Study SOL distribution, holder analysis, and whale tracking
- Enable time-series and event-based analytics on SOL balances

## Important Relationships
- Closely related to `core.fact_token_account_owners` (for historical ownership), `core.fact_events` (for event context), and `core.fact_transfers` (for transfer events)
- Use `core.fact_token_account_owners` to attribute balances to the correct owner
- Use `core.fact_events` for event-level context and protocol interactions
- Use `core.fact_transfers` for asset movement and transfer analytics
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and balance change analysis
- `block_id`, `tx_id`, `tx_index`: For unique identification and joins
- `account_address`, `mint`, `owner`: For account, token, and ownership analytics
- `pre_balance`, `balance`: For before/after balance analysis

{% enddocs %} 