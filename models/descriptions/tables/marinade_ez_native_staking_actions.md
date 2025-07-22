{% docs marinade_ez_native_staking_actions %}

## Description
This table tracks staking actions for accounts that have interacted with Marinade Native Staking at least once. It provides a comprehensive view of native staking activities including deposits, withdrawals, and balance changes, with USD pricing information and validator details, enabling analysis of Marinade's native staking proxy service.

## Key Use Cases
- Analyze native staking patterns through Marinade's proxy service
- Track stake account lifecycle and balance changes
- Monitor validator delegation patterns and performance
- Study user behavior in native staking through Marinade
- Support native staking proxy service performance analysis

## Important Relationships
- Links to `silver.staking_lp_actions_labeled_2` for detailed staking action data
- Connects to `gov.fact_stake_accounts` for stake account information
- References `price.ez_prices_hourly` for USD price conversion
- Provides native staking context for Marinade Finance ecosystem analytics

## Commonly-used Fields
- `block_timestamp`: Essential for time-series analysis and trend detection
- `event_type`: Critical for categorizing different staking activities
- `provider_address`: Key for user analysis and behavior tracking
- `stake_account`: Important for stake account lifecycle tracking
- `pre_tx_staked_balance` and `post_tx_staked_balance`: Critical for balance change analysis
- `validator_name` and `validator_rank`: Key for validator performance analysis

{% enddocs %} 