{% docs fact_stake_pool_actions %}

## Description
This table contains deposit and withdrawal actions with stake pools on the Solana blockchain, including major protocols like Lido, Marinade, and Jito. It tracks liquid staking activities, capturing user interactions with stake pools, staking amounts, and pool-specific metadata. Each row represents a single stake pool action, supporting analytics on liquid staking adoption, yield generation, and staking protocol usage.

## Key Use Cases
- Analyze liquid staking adoption and user behavior
- Track stake pool performance and yield generation
- Study staking protocol usage patterns and preferences
- Monitor staking flows and capital allocation
- Support analytics on DeFi staking ecosystem growth

## Important Relationships
- Closely related to `defi.ez_liquidity_pool_actions` (for liquidity provision), `defi.ez_dex_swaps` (for DEX activity), and `core.ez_transfers` (for token movements)
- Use `defi.ez_liquidity_pool_actions` to analyze liquidity provision in staking-related pools
- Use `defi.ez_dex_swaps` to track trading of liquid staking tokens
- Use `core.ez_transfers` to analyze token movements related to staking
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and staking activity analysis
- `stake_pool_name`, `action`: For pool and action identification
- `amount`: For staking amount analytics
- `address`: For user and pool address analysis
- `succeeded`: For transaction success analysis

## Sample Queries

### Daily liquid staking activity by protocol
```sql
-- Daily liquid staking activity by protocol
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    stake_pool_name AS platform,
    action,
    COUNT(DISTINCT tx_id) AS action_txns,
    COUNT(DISTINCT address) AS unique_users,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount_per_action
FROM solana.defi.fact_stake_pool_actions
WHERE block_timestamp >= CURRENT_DATE - 30
    AND succeeded = true
    AND amount IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 6 DESC;
```

### Stake pool market share analysis
```sql
-- Stake pool market share analysis
WITH pool_totals AS (
    SELECT 
        stake_pool_name,
        SUM(CASE WHEN action = 'deposit' THEN amount ELSE 0 END) AS total_deposits,
        SUM(CASE WHEN action = 'withdraw' THEN amount ELSE 0 END) AS total_withdrawals,
        COUNT(DISTINCT address) AS unique_users,
        COUNT(*) AS total_actions
    FROM solana.defi.fact_stake_pool_actions
    WHERE block_timestamp >= CURRENT_DATE - 90
        AND succeeded = true
        AND amount IS NOT NULL
    GROUP BY stake_pool_name
)
SELECT 
    stake_pool_name,
    total_deposits,
    total_deposits * 100.0 / SUM(total_deposits) OVER () AS deposit_market_share_pct,
    total_withdrawals,
    total_deposits - total_withdrawals AS net_deposits,
    unique_users,
    total_actions,
    total_deposits / NULLIF(total_actions, 0) AS avg_action_size
FROM pool_totals
ORDER BY total_deposits DESC;
```

### User staking behavior patterns
```sql
-- User staking behavior patterns
WITH user_activity AS (
    SELECT 
        address AS user_address,
        COUNT(DISTINCT stake_pool_name) AS pools_used,
        COUNT(DISTINCT action) AS action_types_used,
        COUNT(*) AS total_actions,
        SUM(CASE WHEN action = 'deposit' THEN amount ELSE 0 END) AS total_deposits,
        SUM(CASE WHEN action = 'withdraw' THEN amount ELSE 0 END) AS total_withdrawals,
        MIN(block_timestamp) AS first_action,
        MAX(block_timestamp) AS last_action
    FROM solana.defi.fact_stake_pool_actions
    WHERE succeeded = true
        AND amount IS NOT NULL
    GROUP BY address
)
SELECT 
    CASE 
        WHEN total_deposits < 1 THEN '< 1 SOL'
        WHEN total_deposits < 10 THEN '1-10 SOL'
        WHEN total_deposits < 100 THEN '10-100 SOL'
        WHEN total_deposits < 1000 THEN '100-1K SOL'
        ELSE '1K+ SOL'
    END AS user_tier,
    COUNT(*) AS user_count,
    AVG(total_actions) AS avg_actions_per_user,
    AVG(pools_used) AS avg_pools_used,
    SUM(total_deposits) AS tier_total_deposits,
    SUM(total_withdrawals) AS tier_total_withdrawals
FROM user_activity
GROUP BY user_tier
ORDER BY MIN(total_deposits);
```

### Stake pool performance comparison
```sql
-- Stake pool performance comparison
WITH pool_metrics AS (
    SELECT 
        stake_pool_name,
        COUNT(DISTINCT address) AS unique_users,
        COUNT(*) AS total_actions,
        SUM(CASE WHEN action = 'deposit' THEN amount ELSE 0 END) AS total_deposits,
        SUM(CASE WHEN action = 'withdraw' THEN amount ELSE 0 END) AS total_withdrawals,
        AVG(CASE WHEN action = 'deposit' THEN amount END) AS avg_deposit_size,
        COUNT(CASE WHEN succeeded = false THEN 1 END) * 100.0 / COUNT(*) AS failure_rate_pct
    FROM solana.defi.fact_stake_pool_actions
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND amount IS NOT NULL
    GROUP BY 1
)
SELECT 
    stake_pool_name,
    unique_users,
    total_actions,
    total_deposits,
    total_withdrawals,
    total_deposits - total_withdrawals AS net_flow,
    avg_deposit_size,
    failure_rate_pct,
    total_actions / NULLIF(unique_users, 0) AS actions_per_user
FROM pool_metrics
ORDER BY total_deposits DESC;
```

### Large staking actions monitoring
```sql
-- Large staking actions monitoring (whale activity)
SELECT 
    block_timestamp,
    tx_id,
    stake_pool_name AS platform,
    action,
    address AS user_address,
    amount,
    succeeded
FROM solana.defi.fact_stake_pool_actions
WHERE amount >= 1000  -- Large stakes (1000+ SOL)
    AND block_timestamp >= CURRENT_DATE - 7
ORDER BY amount DESC;
```

{% enddocs %} 