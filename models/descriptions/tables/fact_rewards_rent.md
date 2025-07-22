{% docs fact_rewards_rent %}

## Description
This table contains historical rent rewards data from the Solana blockchain, capturing rent payment distribution to validators and vote accounts that occurred before Solana discontinued rent rewards. This table is no longer updated as Solana has eliminated rent rewards as part of network improvements.

## Key Use Cases
- Analyze historical rent reward distribution and validator income patterns
- Study past rent reward accumulation and validator economics
- Research historical network rent distribution and validator incentives
- Support historical validator performance analysis
- Provide context for understanding Solana's transition away from rent rewards

## Important Relationships
- Links to `gov.fact_validators` through `node_pubkey` for historical validator rent analysis
- Connects to `gov.fact_vote_accounts` through `vote_pubkey` for historical vote account rent analysis
- References `gov.dim_epoch` for historical epoch-based rent analysis
- Provides historical rent reward context for validator economics research

## Commonly-used Fields
- `block_timestamp`: Timestamp when the rent reward was distributed (historical)
- `tx_id`: Unique transaction identifier for the rent reward
- `vote_pubkey`: Vote account that received the rent reward
- `node_pubkey`: Validator node associated with the rent reward
- `reward_amount`: Amount of SOL distributed as rent reward
- `epoch`: Epoch when the rent reward was earned

{% enddocs %} 