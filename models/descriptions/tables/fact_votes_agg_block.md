{% docs fact_votes_agg_block %}

## Description
This table contains aggregated vote data by block, providing a summary view of voting activity and consensus participation across the Solana network. It tracks vote account participation in block validation and consensus, enabling analysis of network consensus patterns and validator voting behavior.

## Key Use Cases
- Analyze consensus participation patterns by block
- Track vote account voting frequency and reliability
- Monitor network consensus health and participation rates
- Study validator voting patterns and network consensus
- Support network health analysis and consensus metrics

## Important Relationships
- Links to `gov.fact_vote_accounts` through `vote_pubkey` for vote account analysis
- Connects to `gov.fact_validators` for validator performance analysis
- References `core.fact_blocks` for block context
- Provides consensus context for network analytics

## Commonly-used Fields
- `block_id`: Block number for the aggregated votes
- `vote_pubkey`: Address of the vote account
- `vote_balance`: Voting balance of the account
- `vote_credits`: Vote credits earned by the account

{% enddocs %} 