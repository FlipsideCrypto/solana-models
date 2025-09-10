{% docs fact_token_burn_actions %}

## Description
This table contains information on all token burn events on the Solana blockchain. It tracks the destruction of tokens across various token standards (SPL, Metaplex, etc.), capturing burn authority actions, token account sources, and burn amounts. Each row represents a single token burn event, supporting analytics on token destruction, supply reduction, and token standard usage.

## Key Use Cases
- Analyze token destruction patterns and supply reduction
- Track burn authority activity and token destruction
- Study token standard adoption (SPL, Metaplex, etc.)
- Monitor token supply management and deflationary mechanisms
- Support analytics on token economics and supply dynamics

## Important Relationships
- Closely related to `defi.fact_token_mint_actions` (for mint events), `core.ez_transfers` (for token movements), and `core.fact_token_balances` (for balance changes)
- Use `defi.fact_token_mint_actions` to analyze token creation and supply increase
- Use `core.ez_transfers` to track token movements before burning
- Use `core.fact_token_balances` to analyze balance changes from burning
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and burn activity analysis
- `mint`, `burn_authority`, `token_account`: For token and authority identification
- `burn_amount`: For supply and value analytics
- `mint_standard_type`: For token standard analysis
- `succeeded`: For transaction success analysis

## Sample Queries

### Daily token burn activity
```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    COUNT(*) AS burn_count,
    COUNT(DISTINCT burn_authority) AS unique_burners,
    COUNT(DISTINCT mint) AS unique_tokens_burned,
    SUM(burn_amount / POW(10, COALESCE(decimal, 0))) AS total_tokens_burned
FROM solana.defi.fact_token_burn_actions
WHERE block_timestamp >= CURRENT_DATE - 30
    AND succeeded = TRUE
GROUP BY 1
ORDER BY 1 DESC;
```

### Top tokens by burn volume
```sql
SELECT 
    mint,
    COUNT(*) AS burn_events,
    SUM(burn_amount / POW(10, COALESCE(decimal, 0))) AS total_burned_tokens,
    COUNT(DISTINCT burn_authority) AS unique_burners,
    COUNT(DISTINCT DATE(block_timestamp)) AS active_days,
    MAX(burn_amount / POW(10, COALESCE(decimal, 0))) AS largest_burn_tokens,
    AVG(burn_amount / POW(10, COALESCE(decimal, 0))) AS avg_burn_size_tokens
FROM solana.defi.fact_token_burn_actions
WHERE block_timestamp >= CURRENT_DATE - 7
    AND succeeded = TRUE
GROUP BY mint
HAVING burn_events > 5
ORDER BY total_burned_tokens DESC
LIMIT 50;
```

### Burn patterns by hour of day
```sql
SELECT 
    EXTRACT(HOUR FROM block_timestamp) AS hour_of_day,
    COUNT(*) AS burn_count,
    SUM(burn_amount / POW(10, COALESCE(decimal, 0))) AS total_burned_tokens,
    AVG(burn_amount / POW(10, COALESCE(decimal, 0))) AS avg_burn_tokens,
    COUNT(DISTINCT burn_authority) AS unique_burners
FROM solana.defi.fact_token_burn_actions
WHERE block_timestamp >= CURRENT_DATE - 7
    AND succeeded = TRUE
GROUP BY 1
ORDER BY 1;
```

### Large token burns (whale activity)
```sql
SELECT 
    block_timestamp,
    tx_id,
    burn_authority,
    mint,
    burn_amount,
    burn_amount / POW(10, COALESCE(decimal, 0)) AS burn_amount_normalized,
    event_type,
    succeeded
FROM solana.defi.fact_token_burn_actions
WHERE block_timestamp >= CURRENT_DATE - 1
    AND burn_amount > 1000000  -- Large raw amounts
    AND succeeded = TRUE
ORDER BY burn_amount DESC
LIMIT 100;
```

### Token burn velocity (rate of burns over time)
```sql
WITH daily_burns AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS date,
        mint,
        SUM(burn_amount / POW(10, COALESCE(decimal, 0))) AS daily_burned_tokens,
        COUNT(*) AS burn_events
    FROM solana.defi.fact_token_burn_actions
    WHERE block_timestamp >= CURRENT_DATE - 30
        AND succeeded = TRUE
    GROUP BY 1, 2
),
burn_velocity AS (
    SELECT 
        mint,
        AVG(daily_burned_tokens) AS avg_daily_burn_tokens,
        STDDEV(daily_burned_tokens) AS stddev_daily_burn_tokens,
        MAX(daily_burned_tokens) AS max_daily_burn_tokens,
        COUNT(DISTINCT date) AS active_days
    FROM daily_burns
    GROUP BY 1
)
SELECT 
    mint,
    avg_daily_burn_tokens,
    stddev_daily_burn_tokens,
    max_daily_burn_tokens,
    active_days,
    stddev_daily_burn_tokens / NULLIF(avg_daily_burn_tokens, 0) AS burn_volatility
FROM burn_velocity
WHERE avg_daily_burn_tokens > 100
ORDER BY avg_daily_burn_tokens DESC;
```
{% enddocs %} 