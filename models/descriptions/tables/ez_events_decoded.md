{% docs ez_events_decoded %}

## Description
This table contains one row per decoded Solana record, mapping detailed program activity, event types, and instruction arguments as recorded on-chain. It only includes decoded data for programs for which we have the IDL (as listed in `core.dim_idls`). It includes extracted accounts, arguments, and error information for each decoded record. Each row represents a decoded event, supporting protocol usage analysis, event tracking, error monitoring, and attribution of accounts and signers.

## Key Use Cases
- Analyze decoded program activity and event types
- Attribute events to accounts, signers, and protocols
- Monitor protocol usage, event flows, and error rates
- Support analytics on composable DeFi, NFT, and governance protocols
- Enable flexible analytics on Solana program interactions and protocol activity

## Important Relationships
- Closely related to `core.fact_decoded_instructions` (for decoded instruction details), `core.fact_events` (for event context), and `core.fact_events_inner` (for inner/CPI events)
- Use `core.fact_decoded_instructions` for detailed instruction and argument analysis
- Use `core.fact_events` and `core.fact_events_inner` for event-level context and protocol interactions
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and event sequencing analysis
- `block_id`, `tx_id`, `index`, `inner_index`: For unique event identification and joins
- `program_id`, `event_type`: For filtering by program or event type
- `decoded_instruction`, `decoded_accounts`, `decoded_args`, `decoding_error`: For detailed event and error analytics
- `signers`, `succeeded`: For user attribution and transaction outcome analysis

{% enddocs %} 