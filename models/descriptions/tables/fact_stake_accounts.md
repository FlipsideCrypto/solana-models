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
- `epoch`: Epoch number for time-series and epoch-based analysis
- `stake_pubkey`: Unique stake account address for account tracking
- `vote_pubkey`: Vote account that stake is delegated to (validator identification)
- `authorized_staker`: Address with staking authority over the account
- `authorized_withdrawer`: Address with withdrawal authority over the account
- `active_stake`: Current active stake amount in SOL
- `activation_epoch`, `deactivation_epoch`: Epochs when stake becomes active/inactive
- `type_stake`: Stake account type and status
- `account_sol`: Total SOL balance in the stake account
- `lockup`: Lockup configuration and restrictions

## Sample Queries

### Current epoch staking summary by delegator
```sql
SELECT 
    authorized_staker,
    COUNT(DISTINCT stake_pubkey) AS num_stake_accounts,
    COUNT(DISTINCT vote_pubkey) AS num_validators_delegated,
    SUM(active_stake) AS total_active_stake_sol
FROM solana.gov.fact_stake_accounts
WHERE epoch = (SELECT MAX(epoch) FROM solana.gov.fact_stake_accounts)
    AND active_stake > 0
GROUP BY authorized_staker
HAVING total_active_stake_sol > 100
ORDER BY total_active_stake_sol DESC
LIMIT 100;
```

### Stake account lifecycle analysis
```sql
SELECT 
    epoch,
    COUNT(DISTINCT stake_pubkey) AS total_stake_accounts,
    SUM(active_stake)  AS total_active_stake_sol,
    COUNT(DISTINCT CASE WHEN activation_epoch = epoch THEN stake_pubkey END) AS newly_activated,
    COUNT(DISTINCT CASE WHEN deactivation_epoch = epoch THEN stake_pubkey END) AS newly_deactivated,
    COUNT(DISTINCT vote_pubkey) AS validators_with_stake
FROM solana.gov.fact_stake_accounts
WHERE epoch >= (SELECT MAX(epoch) - 10 FROM solana.gov.fact_stake_accounts)
GROUP BY epoch
ORDER BY epoch DESC;
```

### Validator stake distribution analysis
```sql
SELECT 
    vote_pubkey,
    COUNT(DISTINCT stake_pubkey) AS delegated_accounts,
    COUNT(DISTINCT authorized_staker) AS unique_delegators,
    SUM(active_stake)  AS total_delegated_sol,
    AVG(active_stake)  AS avg_delegation_sol,
    MIN(activation_epoch) AS earliest_activation,
    MAX(CASE WHEN deactivation_epoch = 18446744073709551615 THEN NULL ELSE deactivation_epoch END) AS latest_deactivation
FROM solana.gov.fact_stake_accounts
WHERE epoch = (SELECT MAX(epoch) FROM solana.gov.fact_stake_accounts)
    AND vote_pubkey IS NOT NULL
    AND active_stake > 0
GROUP BY vote_pubkey
HAVING total_delegated_sol > 1000
ORDER BY total_delegated_sol DESC
LIMIT 50;
```

### Large stake accounts monitoring
```sql
SELECT 
    epoch,
    stake_pubkey,
    vote_pubkey,
    authorized_staker,
    authorized_withdrawer,
    active_stake  AS active_stake_sol,
    account_sol  AS total_balance_sol,
    activation_epoch,
    CASE 
        WHEN deactivation_epoch = 18446744073709551615 THEN 'Active'
        ELSE 'Deactivating at epoch ' || deactivation_epoch
    END AS status
FROM solana.gov.fact_stake_accounts
WHERE epoch = (SELECT MAX(epoch) FROM solana.gov.fact_stake_accounts)
    AND active_stake  >= 10000  -- 10K+ SOL stakes
ORDER BY active_stake DESC
LIMIT 100;
```

{% enddocs %} 