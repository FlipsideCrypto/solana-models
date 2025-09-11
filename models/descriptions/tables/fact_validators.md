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

## Sample Queries

### Top validators by total stake with performance metrics
```sql
SELECT 
    vote_pubkey,
    validator_name,
    SUM(active_stake) / pow(10,9) AS total_stake_sol,
    AVG(commission) AS avg_commission_pct,
    COUNT(DISTINCT epoch) AS epochs_active,
    MAX(epoch) AS last_active_epoch,
    AVG(CASE WHEN delinquent = FALSE THEN 1 ELSE 0 END) * 100 AS uptime_percentage
FROM solana.gov.fact_validators
WHERE epoch >= (SELECT MAX(epoch) - 10 FROM solana.gov.fact_validators)
GROUP BY vote_pubkey, validator_name
HAVING total_stake_sol > 10000
ORDER BY total_stake_sol DESC
LIMIT 50;
```


### Validator geographic distribution and performance
```sql
SELECT 
    data_center_host,
    data_center_key,
    COUNT(DISTINCT vote_pubkey) AS validator_count,
    SUM(active_stake) / pow(10,9) AS total_stake_sol,
    AVG(commission) AS avg_commission,
    AVG(CASE WHEN delinquent = FALSE THEN 1 ELSE 0 END) * 100 AS avg_uptime_pct,
    COUNT(DISTINCT CASE WHEN delinquent = FALSE THEN vote_pubkey END) AS active_validators
FROM solana.gov.fact_validators
WHERE epoch = (SELECT MAX(epoch) FROM solana.gov.fact_validators)
    AND data_center_host IS NOT NULL
GROUP BY data_center_host, data_center_key
HAVING validator_count > 1
ORDER BY total_stake_sol DESC
LIMIT 50;
```

{% enddocs %} 