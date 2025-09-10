{% docs marinade_ez_liquid_staking_actions %}

## Description
This table captures actions related to Marinade liquid staking, including deposits, unstakes, and claims. It provides a unified view of liquid staking activities with USD pricing information, enabling comprehensive analysis of Marinade Finance's liquid staking protocol usage and user behavior patterns.

## Key Use Cases
- Analyze liquid staking deposit and withdrawal patterns
- Track MSOL minting and burning activities
- Monitor claim activities and reward distributions
- Study user behavior in liquid staking protocols
- Support liquid staking protocol performance analysis

## Important Relationships
- Connects to `price.ez_prices_hourly` for USD price conversion
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- Provides liquid staking context for Marinade Finance ecosystem analytics

## Commonly-used Fields
- `block_timestamp`: Essential for time-series analysis and trend detection
- `action_type`: Critical for categorizing different staking activities
- `provider_address`: Key for user analysis and behavior tracking
- `deposit_amount` and `deposit_amount_usd`: Important for value analysis and financial metrics
- `msol_minted` and `msol_burned`: Critical for MSOL token flow analysis

## Sample Queries

### Daily liquid staking deposits by protocol
```sql
-- Daily liquid staking deposits by protocol
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    'marinade' AS platform,
    COUNT(DISTINCT tx_id) AS deposit_txns,
    COUNT(DISTINCT provider_address) AS unique_stakers,
    SUM(deposit_amount) AS sol_staked,
    SUM(deposit_amount_usd) AS usd_staked,
    AVG(deposit_amount) AS avg_stake_size
FROM solana.marinade.ez_liquid_staking_actions
WHERE block_timestamp >= CURRENT_DATE - 30
    AND action_type = 'deposit'
    AND deposit_amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY 1 DESC, 6 DESC;
```

### Protocol market share analysis
```sql
-- Protocol market share analysis
WITH protocol_totals AS (
    SELECT 
        'marinade' AS platform,
        SUM(deposit_amount) AS total_sol_staked,
        COUNT(DISTINCT provider_address) AS unique_stakers,
        COUNT(*) AS total_deposits
    FROM solana.marinade.ez_liquid_staking_actions
    WHERE block_timestamp >= CURRENT_DATE - 90
        AND action_type = 'deposit'
        AND deposit_amount IS NOT NULL
    GROUP BY platform
)
SELECT 
    platform,
    total_sol_staked,
    total_sol_staked * 100.0 / SUM(total_sol_staked) OVER () AS market_share_pct,
    unique_stakers,
    total_deposits,
    total_sol_staked / total_deposits AS avg_deposit_size
FROM protocol_totals
ORDER BY total_sol_staked DESC;
```

### Staker behavior patterns
```sql
-- Staker behavior patterns
WITH staker_activity AS (
    SELECT 
        provider_address AS staker,
        COUNT(DISTINCT action_type) AS actions_used,
        COUNT(*) AS total_actions,
        SUM(CASE WHEN action_type = 'deposit' THEN deposit_amount ELSE 0 END) AS total_sol_staked,
        MIN(block_timestamp) AS first_stake,
        MAX(block_timestamp) AS last_stake,
        COUNT(DISTINCT DATE_TRUNC('month', block_timestamp)) AS active_months
    FROM solana.marinade.ez_liquid_staking_actions
    WHERE deposit_amount IS NOT NULL
    GROUP BY staker
)
SELECT 
    CASE 
        WHEN total_sol_staked < 1 THEN '< 1 SOL'
        WHEN total_sol_staked < 10 THEN '1-10 SOL'
        WHEN total_sol_staked < 32 THEN '10-32 SOL'
        WHEN total_sol_staked < 100 THEN '32-100 SOL'
        ELSE '100+ SOL'
    END AS staker_tier,
    COUNT(*) AS staker_count,
    AVG(total_actions) AS avg_actions_per_staker,
    AVG(actions_used) AS avg_action_types_used,
    SUM(total_sol_staked) AS tier_total_sol
FROM staker_activity
GROUP BY staker_tier
ORDER BY MIN(total_sol_staked);
```

### Exchange rate analysis (mSOL received per SOL)
```sql
-- Exchange rate analysis (mSOL received per SOL)
SELECT 
    'marinade' AS platform,
    'mSOL' AS token_symbol,
    DATE_TRUNC('day', block_timestamp) AS date,
    AVG(msol_minted / NULLIF(deposit_amount, 0)) AS avg_exchange_rate,
    MIN(msol_minted / NULLIF(deposit_amount, 0)) AS min_rate,
    MAX(msol_minted / NULLIF(deposit_amount, 0)) AS max_rate,
    COUNT(*) AS sample_size
FROM solana.marinade.ez_liquid_staking_actions
WHERE deposit_amount > 0 
    AND msol_minted > 0
    AND action_type = 'deposit'
    AND block_timestamp >= CURRENT_DATE - 30
GROUP BY 1, 2, 3
ORDER BY 3 DESC;
```

### Large deposits monitoring (whale activity)
```sql
-- Large deposits monitoring (whale activity)
SELECT 
    block_timestamp,
    tx_id,
    'marinade' AS platform,
    provider_address AS staker,
    deposit_amount AS sol_amount,
    deposit_amount_usd AS sol_amount_usd,
    'mSOL' AS token_symbol,
    msol_minted AS token_amount,
    msol_minted / NULLIF(deposit_amount, 0) AS exchange_rate
FROM solana.marinade.ez_liquid_staking_actions
WHERE deposit_amount >= 100
    AND action_type = 'deposit'
    AND block_timestamp >= CURRENT_DATE - 7
ORDER BY deposit_amount DESC;
```

### Weekly staking momentum
```sql
-- Weekly staking momentum
WITH weekly_deposits AS (
    SELECT 
        DATE_TRUNC('week', block_timestamp) AS week,
        'marinade' AS platform,
        SUM(deposit_amount) AS weekly_sol_staked,
        COUNT(DISTINCT provider_address) AS unique_stakers
    FROM solana.marinade.ez_liquid_staking_actions
    WHERE block_timestamp >= CURRENT_DATE - 84
        AND action_type = 'deposit'
        AND deposit_amount IS NOT NULL
    GROUP BY 1, 2
)
SELECT 
    week,
    platform,
    weekly_sol_staked,
    LAG(weekly_sol_staked) OVER (PARTITION BY platform ORDER BY week) AS prev_week_sol,
    (weekly_sol_staked / NULLIF(LAG(weekly_sol_staked) OVER (PARTITION BY platform ORDER BY week), 0) - 1) * 100 AS week_over_week_pct,
    unique_stakers
FROM weekly_deposits
ORDER BY week DESC, weekly_sol_staked DESC;
```

{% enddocs %} 