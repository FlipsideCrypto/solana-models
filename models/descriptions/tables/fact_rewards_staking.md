{% docs fact_rewards_staking %}

## Description
This table tracks staking rewards distributed to stake accounts on the Solana blockchain. It captures reward events, amounts, and distribution patterns, enabling comprehensive analysis of staking returns, validator performance, and reward distribution across the network.

## Key Use Cases
- Analyze staking reward distribution and returns
- Track validator performance and reward generation
- Study staking participation and reward patterns
- Monitor stake account reward accumulation
- Support staking analytics and return optimization

## Important Relationships
- Links to `gov.fact_stake_accounts` through stake account addresses for reward analysis
- Connects to `gov.fact_validators` through `vote_pubkey` for validator performance
- References `gov.dim_epoch` for epoch-based reward analysis
- Provides reward context for staking analytics

## Commonly-used Fields
- `block_timestamp`: Timestamp when the reward was distributed
- `tx_id`: Unique transaction identifier for the reward
- `stake_account`: Address of the stake account receiving the reward
- `vote_pubkey`: Vote account associated with the reward
- `reward_amount`: Amount of SOL distributed as reward
- `epoch`: Epoch when the reward was earned

{% enddocs %} 