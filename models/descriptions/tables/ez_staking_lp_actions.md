{% docs ez_staking_lp_actions %}

## Description
This table provides a unified view of native staking actions on the Solana blockchain using the official Stake11111111111111111111111111111111111111 program. It consolidates stake delegation, activation, deactivation, and withdrawal operations and enriches them with metadata, USD pricing, and validator information, enabling comprehensive analysis of native staking participation and validator delegation patterns.

## Key Use Cases
- Analyze native staking delegation patterns and validator selection
- Track stake account lifecycle and balance changes with USD values
- Monitor native staking participation and delegation trends
- Study validator stake distribution and concentration
- Support native staking analytics and reward distribution analysis

## Important Relationships
- Links to `gov.fact_staking_lp_actions` for detailed native staking action data
- Connects to `price.ez_prices_hourly` for USD price conversion
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- Provides unified native staking context for validator analytics

## Commonly-used Fields
- `block_timestamp`: Timestamp when the staking action occurred
- `tx_id`: Unique transaction identifier for the staking action
- `stake_account`: Address of the stake account
- `vote_pubkey`: Vote account that the stake is delegated to
- `action`: Type of staking action (e.g., 'delegate', 'deactivate', 'withdraw')
- `amount`: Amount of SOL involved in the action
- `amount_usd`: USD value of the action
- `user`: Address of the user performing the action
- `validator_name`: Name of the validator being delegated to

{% enddocs %} 