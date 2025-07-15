{% docs fact_decoded_instructions %}

## Description
This table contains one row per decoded instruction on the Solana blockchain, including program ID, event type, and detailed instruction metadata as recorded on-chain. It only includes decoded data for programs for which we have the IDL (as listed in `core.dim_idls`). It enables record-level analytics, program usage tracking, and event segmentation for the Solana blockchain. Each row represents a decoded instruction, supporting detailed analysis of protocol interactions, program calls, and event flows.

## Key Use Cases
- Analyze decoded instructions and program calls
- Segment and classify protocol interactions by event type
- Study program usage and protocol adoption
- Support analytics on composable DeFi, NFT, and governance protocols
- Enable detailed event and instruction-level analytics

## Important Relationships
- Closely related to `core.fact_events` (for event context), `core.fact_events_inner` (for inner/CPI events), and `core.fact_transfers` (for transfer events)
- Use `core.fact_events` for event-level context and protocol interactions
- Use `core.fact_events_inner` for nested program calls and composability analysis
- Use `core.fact_transfers` for asset movement and transfer analytics
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and instruction sequencing analysis
- `block_id`, `tx_id`, `index`, `inner_index`: For unique instruction identification and joins
- `program_id`, `event_type`: For filtering by program or event type
- `decoded_instruction`: For detailed instruction analytics
- `signers`: For user attribution and protocol usage analysis

{% enddocs %} 