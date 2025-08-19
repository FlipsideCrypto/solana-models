{% docs fact_events_inner %}

## Description
This table records every event that occurs within inner instructions (Cross-Program Invocations, or CPIs) on the Solana blockchain. Each row represents an event triggered by a program calling another program during transaction execution, capturing the full context of nested program interactions. The table covers all inner events observed on Solana mainnet, with detailed indexing to support analysis of deeply nested program flows. Events are uniquely identified by block, transaction, instruction, and inner instruction indices. This model enables granular tracing of protocol and application logic, supporting advanced analytics on composability, protocol integrations, and on-chain automation.

## Key Use Cases
- Analyze Cross-Program Invocations (CPIs) and nested program calls
- Trace the full execution flow of complex Solana transactions
- Study composability and protocol integrations
- Support advanced analytics on on-chain automation and program-to-program interactions
- Downstream analytics for protocol-specific event flows and composable DeFi/NFT protocols

## Important Relationships
- Closely related to `core.fact_events` (for top-level events), `core.ez_events_decoded` (preferred for decoded instruction details), and `core.ez_transfers` (for transfer events)
- Use `core.fact_events` for top-level program events and instruction execution
- Use `core.ez_events_decoded` for detailed instruction and argument analysis (if program is being decoded)
- Use `core.ez_transfers` for asset movement and transfer analytics
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and event sequencing analysis
- `block_id`, `tx_id`, `instruction_index`, `inner_index`: For unique event identification and joins
- `program_id`, `instruction_program_id`, `event_type`: For filtering by program or event type
- `instruction`: For instruction-level analytics
- `signers`, `succeeded`: For user attribution and transaction outcome analysis

{% enddocs %} 