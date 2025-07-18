{% docs fact_rewards_voting %}

## Description
This table tracks voting rewards distributed to vote accounts on the Solana blockchain. It captures reward events for validator voting participation, enabling analysis of voting reward distribution, validator voting performance, and network consensus participation incentives.

## Key Use Cases
- Analyze voting reward distribution and validator incentives
- Track vote account voting performance and reward generation
- Study voting participation patterns and reward accumulation
- Monitor validator voting reliability and reward patterns
- Support validator performance analysis and incentive optimization

## Important Relationships
- Links to `gov.fact_vote_accounts` through vote account addresses for reward analysis
- Connects to `gov.fact_validators` through `vote_pubkey` for validator performance
- References `gov.dim_epoch` for epoch-based reward analysis
- Provides voting reward context for validator analytics

## Commonly-used Fields
- `block_timestamp`: Timestamp when the voting reward was distributed
- `tx_id`: Unique transaction identifier for the voting reward
- `vote_pubkey`: Address of the vote account receiving the reward
- `node_pubkey`: Validator node associated with the vote account
- `reward_amount`: Amount of SOL distributed as voting reward
- `epoch`: Epoch when the voting reward was earned

{% enddocs %} 