{% docs fact_validators %}

## Description
This table contains comprehensive validator data by epoch, sourced from the Validators.app API. It tracks validator performance, stake distribution, geographic location, and operational metrics, providing detailed insights into Solana's validator network and supporting staking analytics and network health monitoring.

## Key Use Cases
- Analyze validator performance and stake distribution across epochs
- Track validator geographic distribution and data center locations
- Monitor validator uptime, commission rates, and operational status
- Study staking patterns and validator selection behavior
- Support network health analysis and validator ranking

## Important Relationships
- Links to `gov.dim_epoch` via `epoch` for epoch-based validator analysis
- Connects to `gov.fact_vote_accounts` through `vote_pubkey` for vote account analysis
- References `gov.fact_stake_accounts` for stake distribution analysis
- Provides validator context for `gov.fact_rewards_staking` and `gov.fact_rewards_voting`

## Commonly-used Fields
- `epoch`: The epoch when validator data was recorded
- `node_pubkey`: Account for the validator node
- `vote_pubkey`: Vote account for the validator
- `active_stake`: Active stake in SOL delegated to the validator
- `commission`: Percentage of rewards payout to the vote account
- `delinquent`: Status whether the validator is offline/delinquent
- `validator_name`: Name of the validator
- `data_center_key`: Identifier for the data center
- `data_center_host`: Host for the data center
- `latitude` and `longitude`: Geographic coordinates of data center
- `software_version`: Solana mainnet version

{% enddocs %} 