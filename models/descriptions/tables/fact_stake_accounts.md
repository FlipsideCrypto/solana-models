{% docs fact_stake_accounts %}

## Description
This table tracks stake accounts on the Solana blockchain, capturing stake delegation, activation, deactivation, and balance changes. Stake accounts represent delegated SOL that contributes to network security and validator voting power, enabling comprehensive staking analytics and delegation pattern analysis.

## Key Use Cases
- Track stake delegation patterns and validator selection
- Analyze stake account lifecycle and balance changes
- Monitor staking participation and delegation trends
- Study validator stake distribution and concentration
- Support staking analytics and reward distribution analysis

## Important Relationships
- Links to `gov.fact_validators` through `vote_pubkey` for validator-stake mapping
- Connects to `gov.fact_vote_accounts` for vote account analysis
- References `gov.fact_rewards_staking` for reward distribution analysis
- Provides stake context for `gov.fact_rewards_voting`

## Commonly-used Fields
- `block_timestamp`: Timestamp when the stake account event occurred
- `tx_id`: Unique transaction identifier for the stake account event
- `stake_account`: Address of the stake account
- `vote_pubkey`: Vote account that the stake is delegated to
- `authority`: Address with authority over the stake account
- `stake_balance`: Current stake balance of the account
- `stake_type`: Type of stake (e.g., 'active', 'inactive', 'activating', 'deactivating')
- `lockup_epoch`: Epoch when stake lockup expires
- `custodian`: Address with custody over the stake account

{% enddocs %} 