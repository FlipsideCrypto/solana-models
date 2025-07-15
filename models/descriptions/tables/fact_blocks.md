{% docs fact_blocks %}

## Description
This table contains one record per block produced on the Solana blockchain, capturing block-level metadata including block identifiers, timestamps, hashes, network and chain information, and references to previous blocks. The table covers all finalized blocks on Solana mainnet and is updated as new blocks are processed. Data is sourced from on-chain block logs and normalized for analytics, supporting chain continuity and block production analysis.

## Key Use Cases
- Block-level analytics and network monitoring
- Chain continuity and fork analysis
- Time-series analysis of block production and network activity
- Joining with transaction and event tables for multi-level analytics
- Block explorer and dashboard backends

## Important Relationships
- Each block is linked to its predecessor via `previous_block_id` and `previous_block_hash` fields, supporting chain continuity analysis
- Joins with `core.fact_transactions` for transaction-level context 

## Commonly-used Fields
- `block_id`: Unique identifier for each block, used for joins and traceability
- `block_timestamp`: For time-series and block production analysis

{% enddocs %} 