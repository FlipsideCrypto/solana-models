{% docs dim_epoch %}

## Description
This table contains epoch information for the Solana blockchain, including the block range for each epoch. It serves as a dimension table that provides epoch context for governance and staking analytics, enabling time-based analysis across different epochs and supporting epoch-level aggregations.

## Key Use Cases
- Provide epoch context for governance and staking analytics
- Enable epoch-based filtering and grouping of blockchain data
- Support time-series analysis across different epochs
- Facilitate epoch-level aggregations and reporting
- Track epoch progression and block range changes

## Important Relationships
- Links to `gov.fact_validators` via `epoch` for validator performance analysis
- Connects to `gov.fact_proposal_votes` through epoch context for governance timeline analysis
- References `gov.fact_rewards_staking` and `gov.fact_rewards_voting` for epoch-based reward tracking
- Provides epoch context for `gov.fact_stake_accounts` and `gov.fact_vote_accounts`

## Commonly-used Fields
- `epoch`: The epoch number identifier
- `start_block`: The first block number in the epoch
- `end_block`: The last block number in the epoch

{% enddocs %} 