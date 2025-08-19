{% docs fact_events %}

## Description
This table records every event emitted by on-chain Solana programs during transaction execution. Each row represents a single event, including its type, the program that emitted it, and its position within the transaction. The table covers all events observed on Solana mainnet, including protocol-level and application-level activity. Events are uniquely identified by block, transaction, and event index. This model enables detailed analysis of program behavior, user actions, and protocol interactions at the event level, and is central to understanding the full scope of on-chain activity.

## Key Use Cases
- Analyze program interactions and instruction execution flow
- Track specific program events, methods, or user actions
- Protocol usage analytics and event-level activity monitoring
- Avoid complex JSON array parsing from `core.fact_transactions` by using pre-parsed event fields
- Downstream analytics for protocol-specific event flows and decoded instructions

## Important Relationships
- Closely related to `core.fact_events_inner` (for inner/CPI events), `core.ez_events_decoded` (preferred for decoded instruction details), and `core.ez_transfers` (for transfer events)
- Use `core.fact_events_inner` to analyze Cross-Program Invocations (CPIs) and nested program calls
- Use `core.ez_events_decoded` for detailed instruction and argument analysis (if program is being decoded)
- Use `core.ez_transfers` for asset movement and transfer analytics
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and event sequencing analysis
- `block_id`, `tx_id`, `index`: For unique event identification and joins
- `program_id`, `event_type`: For filtering by program or event type
- `instruction`, `inner_instruction`: For instruction-level analytics
- `signers`, `succeeded`: For user attribution and transaction outcome analysis

{% enddocs %} 