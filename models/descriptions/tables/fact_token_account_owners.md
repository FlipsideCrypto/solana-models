{% docs fact_token_account_owners %}

## Description
This table contains one row per token account address, recording the range of blocks during which a given owner controlled the account. It enables historical tracking of token account ownership on the Solana blockchain, supporting attribution of token balances and transfers to the correct owner at any point in time. Each row represents a unique ownership period for a token account, with start and end block identifiers. Null end_block_id indicates current ownership.

## Key Use Cases
- Attribute token balances and transfers to the correct owner at any point in time
- Analyze historical changes in token account ownership
- Support analytics on token flows, DeFi protocol activity, and wallet histories
- Study token distribution, holder analysis, and whale tracking
- Enable time-series and event-based analytics on token account ownership

## Important Relationships
- Closely related to `core.fact_token_balances` (for balance changes), `core.fact_sol_balances` (for SOL balances), and `core.ez_transfers` (for transfer events)
- Use `core.fact_token_balances` to analyze token balance changes and account activity
- Use `core.fact_sol_balances` for SOL balance analytics
- Use `core.ez_transfers` for asset movement and transfer analytics
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `account_address`, `owner`: For account and ownership analytics
- `start_block_id`, `end_block_id`: For historical ownership period analysis
- `fact_token_account_owners_id`: For unique identification and joins

{% enddocs %} 