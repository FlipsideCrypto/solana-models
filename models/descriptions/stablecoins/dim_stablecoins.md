{% docs dim_stablecoins_table_doc %}

## What

This table provides a dimensional view of verified stablecoins across Solana. It consolidates stablecoin metadata from various sources to create a unified reference table for identifying and analyzing stablecoin tokens.

## Key Use Cases

- Identifying stablecoin tokens in transaction and event data
- Filtering DeFi activities to stablecoin-only transactions
- Analyzing stablecoin adoption and distribution
- Tracking verified stablecoin contracts across chains
- Building stablecoin-specific metrics and dashboards

## Important Relationships

- **Join with defi.ez_stablecoins_supply**: Use `token_address` for supply metrics

## Commonly-used Fields

- `token_address`: Unique stablecoin token contract address
- `symbol`: Token symbol (e.g., USDC, USDT, DAI)
- `name`: Full token name
- `label`: Combined symbol and name, as a stablecoin unique identifier
- `decimals`: Number of decimal places for the token
- `is_verified`: Verification status

## Sample queries

```sql
-- Get unique stablecoins
SELECT 
    label AS stablecoin,
    COUNT(*) AS token_count
FROM solana..defi.dim_stablecoins
GROUP BY 1
ORDER BY 2 DESC;

-- Get all USDC variants
SELECT 
    token_address,
    symbol,
    name,
    decimals
FROM solana.defi.dim_stablecoins
WHERE symbol LIKE '%USDC%'
ORDER BY symbol;

-- Check if specific address is a stablecoin
SELECT 
    token_address,
    label,
    decimals
FROM solana.defi.dim_stablecoins
WHERE token_address = LOWER('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v');
```

{% enddocs %}

{% docs dim_stablecoins_token_address %}

The unique token address of the stablecoin token.

Example: 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'

{% enddocs %}

{% docs dim_stablecoins_symbol %}

The symbol identifier for the stablecoin token.

Example: 'USDC'

{% enddocs %}

{% docs dim_stablecoins_name %}

The full name of the stablecoin token.

Example: 'USD Coin'

{% enddocs %}

{% docs dim_stablecoins_label %}

A combined display label containing both symbol and name.

Example: 'USDC: USD Coin'

{% enddocs %}

{% docs dim_stablecoins_decimals %}

The number of decimal places used by the token address.

Example: 6

{% enddocs %}

{% docs dim_stablecoins_is_verified %}

Indicates whether the stablecoin is verified by the Flipside team.

Example: true

{% enddocs %}

{% docs dim_stablecoins_is_verified_modified_timestamp %}

Indicates when the stablecoin was verified by the Flipside team.

Example: true

{% enddocs %}