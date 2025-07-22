{% docs fact_block_production %}

## Description
This table tracks block production events on the Solana blockchain, capturing which validators produced blocks and when. It provides insights into network performance, validator participation in consensus, and block production distribution across the validator set.

## Key Use Cases
- Analyze block production distribution across validators
- Track validator participation in network consensus
- Monitor network performance and block production efficiency
- Study validator reliability and block production patterns
- Support network health analysis and validator performance metrics

## Important Relationships
- Links to `gov.fact_validators` through `node_pubkey` for validator performance analysis
- Connects to `gov.fact_vote_accounts` for vote account analysis
- References `core.fact_blocks` for block context
- Provides block production context for validator analytics

## Commonly-used Fields
- `block_timestamp`: Timestamp when the block was produced
- `block_id`: Block number that was produced
- `node_pubkey`: Address of the validator that produced the block
- `vote_pubkey`: Vote account associated with the block producer
- `epoch`: Epoch when the block was produced

{% enddocs %} 