{% docs fact_token_mint_actions %}

## Description
This table contains information on all token mint events on the Solana blockchain. It tracks the creation of new tokens across various token standards (SPL, Metaplex, etc.), capturing mint authority actions, token account destinations, and mint amounts. Each row represents a single token mint event, supporting analytics on token creation, supply dynamics, and token standard usage.

## Key Use Cases
- Analyze token creation patterns and supply dynamics
- Track mint authority activity and token issuance
- Study token standard adoption (SPL, Metaplex, etc.)
- Monitor new token launches and initial distributions
- Support analytics on token economics and supply management

## Important Relationships
- Closely related to `defi.fact_token_burn_actions` (for burn events), `core.ez_transfers` (for token movements), and `core.fact_token_balances` (for balance changes)
- Use `defi.fact_token_burn_actions` to analyze token destruction and supply reduction
- Use `core.ez_transfers` to track token movements after minting
- Use `core.fact_token_balances` to analyze balance changes from minting
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and mint activity analysis
- `mint`, `mint_authority`, `token_account`: For token and authority identification
- `mint_amount`: For supply and value analytics
- `mint_standard_type`: For token standard analysis
- `succeeded`: For transaction success analysis

## Sample Queries

### Daily token minting activity
```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    COUNT(*) AS mint_count,
    COUNT(DISTINCT mint_authority) AS unique_minters,
    COUNT(DISTINCT mint) AS unique_tokens_minted,
    SUM(mint_amount / POW(10, COALESCE(decimal, 0))) AS total_tokens_minted
FROM solana.defi.fact_token_mint_actions
WHERE block_timestamp >= CURRENT_DATE - 30
    AND succeeded = TRUE
GROUP BY 1
ORDER BY 1 DESC;
```

### Top minting authorities by volume
```sql
SELECT 
    mint_authority,
    COUNT(DISTINCT mint) AS unique_tokens,
    COUNT(*) AS mint_events,
    SUM(mint_amount / POW(10, COALESCE(decimal, 0))) AS total_minted_tokens,
    AVG(mint_amount / POW(10, COALESCE(decimal, 0))) AS avg_mint_size_tokens,
    MAX(mint_amount / POW(10, COALESCE(decimal, 0))) AS largest_mint_tokens,
    COUNT(DISTINCT DATE(block_timestamp)) AS active_days
FROM solana.defi.fact_token_mint_actions
WHERE block_timestamp >= CURRENT_DATE - 7
    AND succeeded = TRUE
GROUP BY mint_authority
HAVING mint_events > 10
ORDER BY total_minted_tokens DESC
LIMIT 50;
```

### Token supply expansion analysis
```sql
WITH mint_burn_comparison AS (
    SELECT 
        m.mint,
        COALESCE(SUM(m.mint_amount / POW(10, COALESCE(m.decimal, 0))), 0) AS total_minted,
        COALESCE(b.total_burned, 0) AS total_burned
    FROM solana.defi.fact_token_mint_actions m
    LEFT JOIN (
        SELECT 
            mint,
            SUM(burn_amount / POW(10, COALESCE(decimal, 0))) AS total_burned
        FROM solana.defi.fact_token_burn_actions
        WHERE block_timestamp >= CURRENT_DATE - 7
            AND succeeded = TRUE
        GROUP BY 1
    ) b ON m.mint = b.mint
    WHERE m.block_timestamp >= CURRENT_DATE - 7
        AND m.succeeded = TRUE
    GROUP BY 1, b.total_burned
)
SELECT 
    mint,
    total_minted,
    total_burned,
    total_minted - total_burned AS net_supply_change,
    CASE 
        WHEN total_burned = 0 THEN 'Pure Inflation'
        WHEN total_minted > total_burned THEN 'Net Inflation'
        WHEN total_minted < total_burned THEN 'Net Deflation'
        ELSE 'Balanced'
    END AS supply_dynamics
FROM mint_burn_comparison
WHERE total_minted > 0 OR total_burned > 0
ORDER BY ABS(total_minted - total_burned) DESC
LIMIT 100;
```

### Minting velocity and patterns
```sql
WITH hourly_mints AS (
    SELECT 
        DATE_TRUNC('hour', block_timestamp) AS hour,
        mint,
        COUNT(*) AS mint_events,
        SUM(mint_amount / POW(10, COALESCE(decimal, 0))) AS hourly_minted_tokens
    FROM solana.defi.fact_token_mint_actions
    WHERE block_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
        AND succeeded = TRUE
    GROUP BY 1, 2
)
SELECT 
    hour,
    COUNT(DISTINCT mint) AS active_tokens,
    SUM(mint_events) AS total_mint_events,
    SUM(hourly_minted_tokens) AS total_minted_tokens,
    AVG(hourly_minted_tokens) AS avg_token_mint_amount,
    MAX(hourly_minted_tokens) AS max_token_mint_amount
FROM hourly_mints
GROUP BY 1
ORDER BY 1 DESC;
```

### Large minting events (potential inflation events)
```sql
SELECT 
    block_timestamp,
    tx_id,
    mint_authority,
    mint,
    mint_amount,
    mint_amount / POW(10, COALESCE(decimal, 0)) AS mint_amount_normalized,
    event_type,
    succeeded
FROM solana.defi.fact_token_mint_actions
WHERE block_timestamp >= CURRENT_DATE - 1
    AND mint_amount > 1000000  -- Large raw amounts
    AND succeeded = TRUE
ORDER BY mint_amount DESC
LIMIT 100;
```

### Token creation and initial mints
```sql
WITH first_mints AS (
    SELECT 
        mint,
        MIN(block_timestamp) AS first_mint_time,
        FIRST_VALUE(mint_authority) OVER (
            PARTITION BY mint 
            ORDER BY block_timestamp
        ) AS initial_minter,
        FIRST_VALUE(mint_amount / POW(10, COALESCE(decimal, 0))) OVER (
            PARTITION BY mint 
            ORDER BY block_timestamp
        ) AS initial_supply_normalized
    FROM solana.defi.fact_token_mint_actions
    WHERE block_timestamp >= CURRENT_DATE - 7
        AND succeeded = TRUE
)
SELECT DISTINCT
    DATE_TRUNC('day', first_mint_time) AS launch_date,
    mint,
    initial_minter,
    initial_supply_normalized,
    first_mint_time
FROM first_mints
ORDER BY first_mint_time DESC
LIMIT 100;
```

{% enddocs %} 