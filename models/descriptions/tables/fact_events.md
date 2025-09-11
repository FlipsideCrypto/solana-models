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

## Sample Queries

### Event distribution by program with inner instruction metrics
```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    program_id,
    COUNT(*) AS event_count,
    COUNT(DISTINCT tx_id) AS unique_transactions,
    AVG(ARRAY_SIZE(inner_instruction_events)) AS avg_inner_events,
    MAX(ARRAY_SIZE(inner_instruction_events)) AS max_inner_events
FROM solana.core.fact_events
WHERE block_timestamp >= CURRENT_DATE - 7
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;
```

### Simple event count by program
```sql
SELECT 
    program_id,
    COUNT(*) AS total_events
FROM solana.core.fact_events
WHERE block_timestamp >= CURRENT_DATE - 1
GROUP BY program_id
ORDER BY total_events DESC
LIMIT 20;
```

### Recent events with basic details
```sql
SELECT 
    block_timestamp,
    tx_id,
    program_id,
    instruction_index,
    data
FROM solana.core.fact_events
WHERE block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
ORDER BY block_timestamp DESC
LIMIT 100;
```

{% enddocs %} 