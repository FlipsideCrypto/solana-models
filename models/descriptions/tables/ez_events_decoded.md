{% docs ez_events_decoded %}

## Description
This table contains one row per decoded Solana record, mapping detailed program activity, event types, and instruction arguments as recorded on-chain. It only includes decoded data for programs for which we have the IDL (as listed in `core.dim_idls`). This table contains all the information in `core.fact_decoded_instructions`, but with additional extracted fields such as `decoded_accounts`, `decoded_args`, and `decoding_error`. For most analytics use cases, this table is preferred over `core.fact_decoded_instructions` due to its richer, more accessible structure. Each row represents a decoded event, supporting protocol usage analysis, event tracking, error monitoring, and attribution of accounts and signers.

## Key Use Cases
- Analyze decoded program activity and event types
- Attribute events to accounts, signers, and protocols
- Monitor protocol usage, event flows, and error rates
- Support analytics on composable DeFi, NFT, and governance protocols
- Enable flexible analytics on Solana program interactions and protocol activity

## Important Relationships
- Closely related to `core.fact_decoded_instructions` (for raw decoded instruction details), `core.fact_events` (for event context), and `core.fact_events_inner` (for inner/CPI events)
- Use `core.fact_decoded_instructions` for raw instruction data if needed, but prefer this table for most analytics
- Use `core.fact_events` and `core.fact_events_inner` for event-level context and protocol interactions
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and event sequencing analysis
- `block_id`, `tx_id`, `index`, `inner_index`: For unique event identification and joins
- `program_id`, `event_type`: For filtering by program or event type
- `decoded_instruction`, `decoded_accounts`, `decoded_args`, `decoding_error`: For detailed event and error analytics
- `signers`, `succeeded`: For user attribution and transaction outcome analysis

## Sample Queries

### Daily event activity by program
```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    program_id,
    COUNT(*) AS event_count,
    COUNT(DISTINCT tx_id) AS unique_transactions,
    COUNT(DISTINCT signers[0]::STRING) AS unique_signers,
    COUNT(DISTINCT event_type) AS unique_event_types
FROM solana.core.ez_events_decoded
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
```

### Most common event types with decoded data
```sql
SELECT 
    program_id,
    event_type,
    decoded_instruction:name::STRING AS instruction_name,
    COUNT(*) AS occurrences,
    COUNT(DISTINCT signers[0]::STRING) AS unique_signers
FROM solana.core.ez_events_decoded
WHERE block_timestamp >= CURRENT_DATE - 7
    AND decoded_instruction IS NOT NULL
GROUP BY 1, 2, 3
HAVING occurrences > 100
ORDER BY occurrences DESC;
```



{% enddocs %} 