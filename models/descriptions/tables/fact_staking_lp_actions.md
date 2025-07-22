{% docs fact_staking_lp_actions %}

## Description
This table tracks native staking actions on the Solana blockchain using the official Stake11111111111111111111111111111111111111 program. It captures stake delegation, activation, deactivation, and withdrawal operations directly to validators, enabling comprehensive analysis of native staking participation and validator delegation patterns.

## Key Use Cases
- Analyze native staking delegation patterns and validator selection
- Track stake account lifecycle and balance changes
- Monitor native staking participation and delegation trends
- Study validator stake distribution and concentration
- Support native staking analytics and reward distribution analysis

## Important Relationships
- Links to `gov.fact_stake_accounts` through stake account addresses for staking analysis
- Connects to `gov.fact_validators` for validator performance context
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- Provides native staking context for validator analytics

## Commonly-used Fields
- `block_timestamp`: Timestamp when the staking action occurred
- `tx_id`: Unique transaction identifier for the staking action
- `stake_account`: Address of the stake account
- `vote_pubkey`: Vote account that the stake is delegated to
- `action`: Type of staking action (e.g., 'delegate', 'deactivate', 'withdraw')
- `amount`: Amount of SOL involved in the action
- `user`: Address of the user performing the action

{% enddocs %} 