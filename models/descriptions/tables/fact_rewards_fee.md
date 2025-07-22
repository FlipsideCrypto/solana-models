{% docs fact_rewards_fee %}

## Description
This table tracks fee rewards distributed on the Solana blockchain, capturing transaction fee distribution to validators and vote accounts. It enables analysis of fee reward patterns, validator fee income, and network fee distribution across the validator set.

## Key Use Cases
- Analyze fee reward distribution and validator income
- Track validator fee generation and reward patterns
- Study network fee distribution and validator incentives
- Monitor fee reward accumulation and validator economics
- Support validator performance and fee optimization analysis

## Important Relationships
- Links to `gov.fact_validators` through `node_pubkey` for validator fee analysis
- Connects to `gov.fact_vote_accounts` through `vote_pubkey` for vote account fee analysis
- References `gov.dim_epoch` for epoch-based fee analysis
- Provides fee reward context for validator economics

## Commonly-used Fields
- `block_timestamp`: Timestamp when the fee reward was distributed
- `tx_id`: Unique transaction identifier for the fee reward
- `vote_pubkey`: Vote account receiving the fee reward
- `node_pubkey`: Validator node associated with the fee reward
- `reward_amount`: Amount of SOL distributed as fee reward
- `epoch`: Epoch when the fee reward was earned

{% enddocs %} 