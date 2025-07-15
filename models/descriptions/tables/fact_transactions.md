{% docs fact_transactions %}

## Description
This table contains one record per transaction on the Solana blockchain, capturing high-level transaction metadata as recorded on-chain. Each record includes block timestamp, transaction identifiers, account and token balance changes, program instructions, and execution metadata for every transaction processed on Solana mainnet. The table covers all finalized transactions, both successful and failed, and is updated as new blocks are processed. Data is sourced from on-chain transaction logs and normalized for analytics.

## Key Use Cases
- Transaction-level analytics and protocol usage tracking
- Fee analysis and cost monitoring for Solana transactions
- Wallet and account activity analysis
- Token and asset movement tracking
- Program and instruction usage statistics
- Success/failure rate analysis for transactions
- Building time-series dashboards for network activity

## Important Relationships
- Each transaction is linked to its corresponding block in `core.fact_blocks` via the `block_id` field
- Used as a source for `core.fact_events`, `core.fact_transfers`, and other gold-level models for event, transfer, and program analytics
- Joins with `core.fact_blocks` for block-level context and with `core.fact_token_balances` for token movement analysis
- Downstream models may use `tx_id` to join with logs, events, and decoded instructions tables
- Many curated and derived tables are built from this model for specialized analytics. For example, use `core.fact_events` and `core.fact_events_inner` for instruction/event-level analysis, or `core.fact_transfers` for transfer analytics. When possible, prefer these curated tables over querying the large `core.fact_transactions` table directly, as they are optimized for specific analytical use cases.

## Commonly-used Fields
- `tx_id`: Unique identifier for each transaction, used for joins and traceability
- `block_id` and `block_timestamp`: For time-series and block-level analysis
- `signers`: Key for wallet attribution and user activity analysis
- `fee`: For cost and economic analysis
- `succeeded`: Indicates transaction success/failure
- `account_keys`, `pre_balances`, `post_balances`: For account-level balance change analysis
- `instructions`, `inner_instructions`: For program and protocol usage analytics
- `log_messages`: For debugging and advanced protocol analysis

{% enddocs %} 