{% docs fact_sol_daily_balances %}

## Description
This table provides daily snapshots of native SOL balances for each account on the Solana blockchain. It creates a complete daily time series by forward-filling the most recent balance when there's no activity on a given day, ensuring every account has a balance record for each date since their first transaction. When multiple balance updates occur within a day, only the last balance is retained. The table tracks native SOL only (mint address: So11111111111111111111111111111111111111111). 

**Important:** If an account is left with a 0 balance at the end of the day, it is not included in the table. This means accounts will "disappear" from the daily snapshots when their balance reaches zero and "reappear" when they receive SOL again.

## Key Use Cases
- **Daily balance analysis**: Track SOL holdings over time for accounts, wallets, and protocols
- **Portfolio tracking**: Monitor SOL balance changes and trends for specific addresses
- **Whale watching**: Identify large SOL holders and track their balance movements
- **Protocol analysis**: Analyze SOL reserves and treasury balances for DeFi protocols
- **Time series analytics**: Perform historical balance analysis and trend identification
- **Snapshot reporting**: Generate point-in-time balance reports for any historical date

## Important Relationships
- Sources data from `core.fact_sol_balances` which contains all SOL balance changes
- Uses `crosschain.core.dim_dates` for generating complete daily time series
- Complements `core.fact_token_daily_balances` which handles SPL token balances
- Related to `core.ez_transfers` for understanding SOL movement patterns

## Commonly-used Fields
- `balance_date`: Essential for time-based analysis and filtering to specific dates
- `account`: Core field for account-specific balance tracking and wallet analysis  
- `amount`: The SOL balance amount (in decimal SOL, not lamports) for value calculations
- `last_balance_change`: Critical for understanding when balances were last updated (only tracks dates when account had a positive balance)
- `balance_changed_on_date`: Key for filtering to only dates with actual balance activity

## Sample Queries

### Get current SOL balance for a specific account
```sql
SELECT 
    account,
    amount as sol_balance,
    last_balance_change
FROM solana.core.fact_sol_daily_balances 
WHERE account = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
    AND balance_date = CURRENT_DATE() - 1
```

### Track SOL balance changes over time for an account
```sql
SELECT 
    balance_date,
    amount as sol_balance,
    balance_changed_on_date
FROM solana.core.fact_sol_daily_balances 
WHERE account = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
    AND balance_date >= CURRENT_DATE() - 30
ORDER BY balance_date DESC
```

### Find accounts with largest SOL balances on a specific date
```sql
SELECT 
    account,
    amount as sol_balance
FROM solana.core.fact_sol_daily_balances 
WHERE balance_date = '2024-01-01'
    AND amount > 0
ORDER BY amount DESC
LIMIT 100
```

### Analyze SOL balance distribution
```sql
SELECT 
    CASE 
        WHEN amount >= 10000 THEN '10K+ SOL'
        WHEN amount >= 1000 THEN '1K-10K SOL'
        WHEN amount >= 100 THEN '100-1K SOL'
        WHEN amount >= 10 THEN '10-100 SOL'
        ELSE '<10 SOL'
    END as balance_tier,
    COUNT(*) as account_count,
    SUM(amount) as total_sol
FROM solana.core.fact_sol_daily_balances 
WHERE balance_date = CURRENT_DATE() - 1
GROUP BY 1
ORDER BY total_sol DESC
```

{% enddocs %}
