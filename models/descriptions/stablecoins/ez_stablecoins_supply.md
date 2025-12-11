{% docs ez_stablecoins_supply_table_doc %}

## What

This table provides daily supply metrics for verified stablecoins across Solana. It tracks total supply, mint/burn activity, and distribution across various DeFi protocols and platforms. Historical data available, starting from `2025-06-01`.

## Key Use Cases

- Tracking stablecoin supply growth and contraction over time
- Analyzing stablecoin distribution and TVL across DeFi protocols
- Monitoring mint and burn events for supply changes
- Identifying stablecoin liquidity concentration in specific venues
- Building supply-based metrics and charts

## Important Relationships

- **Join with defi.dim_stablecoins**: Use `contract_address` for stablecoin metadata
- **Join with price.ez_prices_hourly**: Use `contract_address` for price data

## Commonly-used Fields

- `block_date`: Date of the supply snapshot
- `contract_address`: Stablecoin token contract address
- `symbol`: Token symbol (e.g., USDC, USDT, DAI)
- `total_supply`: Total supply of the stablecoin, based on direct `totalSupply` contract functions calls
- `amount_minted`: Cumulative amount minted
- `amount_burned`: Cumulative amount burned
- `amount_in_bridges`: Amount held in Bridge vaults
- `amount_in_dex_liquidity_pools`: Amount held in DEX liquidity pools
- `amount_in_lending_pools`: Amount deposited in lending protocols
- `amount_in_cex`: Amount held in centralized exchange addresses

## Sample queries

```sql
-- Latest supply metrics by stablecoin
SELECT 
    label AS stablecoin,
    total_supply,
    amount_in_dex_liquidity_pools,
    amount_in_lending_pools
FROM <blockchain_name>.defi.ez_stablecoins_supply
WHERE block_date = CURRENT_DATE - 1
ORDER BY total_supply DESC;

-- Daily supply changes for a specific stablecoin
SELECT 
    block_date,
    symbol,
    total_supply,
    amount_minted - LAG(amount_minted) OVER (PARTITION BY contract_address ORDER BY block_date) AS daily_minted,
    amount_burned - LAG(amount_burned) OVER (PARTITION BY contract_address ORDER BY block_date) AS daily_burned
FROM <blockchain_name>.defi.ez_stablecoins_supply
WHERE symbol = 'USDC'
    AND block_date >= CURRENT_DATE - 30
ORDER BY block_date DESC;

-- Stablecoin distribution analysis
SELECT 
    block_date,
    label AS stablecoin,
    amount_in_bridges / NULLIF(total_supply, 0) AS pct_in_bridge,
    amount_in_dex_liquidity_pools / NULLIF(total_supply, 0) AS pct_in_dex,
    amount_in_lending_pools / NULLIF(total_supply, 0) AS pct_in_lending,
    amount_in_cex / NULLIF(total_supply, 0) AS pct_in_cex
FROM <blockchain_name>.defi.ez_stablecoins_supply
WHERE block_date = CURRENT_DATE - 1
    AND total_supply > 0
ORDER BY total_supply DESC;
```

{% enddocs %}

{% docs ez_stablecoins_supply_block_date %}

The date of the daily supply snapshot. This corresponds with the MAX block_number from the previous day.

Example: '2025-06-10'

{% enddocs %}

{% docs ez_stablecoins_supply_token_address %}

The address of the stablecoin token.

Example: '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'

{% enddocs %}

{% docs ez_stablecoins_supply_symbol %}

The symbol identifier for the stablecoin token.

Example: 'USDC'

{% enddocs %}

{% docs ez_stablecoins_supply_name %}

The full name of the stablecoin token.

Example: 'USD Coin'

{% enddocs %}

{% docs ez_stablecoins_supply_label %}

A combined display label containing both symbol and name, as a stablecoin unique identifier.

Example: 'USDC: USD Coin'

{% enddocs %}

{% docs ez_stablecoins_supply_decimals %}

The number of decimal places used by the token address.

Example: 6

{% enddocs %}

{% docs ez_stablecoins_supply_total_supply %}

The total supply of the stablecoin on this blockchain as of the block_date.

Example: 1500000000

{% enddocs %}

{% docs ez_stablecoins_supply_total_holders %}

The total number of unique addresses holding the stablecoin on this blockchain as of the block_date.

Example: 750000

{% enddocs %}

{% docs ez_stablecoins_supply_amount_minted %}

The amount of tokens minted daily.

Example: 2000000000

{% enddocs %}

{% docs ez_stablecoins_supply_amount_burned %}

The amount of tokens burned daily.

Example: 500000000

{% enddocs %}

{% docs ez_stablecoins_supply_amount_transferred %}

The amount of tokens transferred daily.

Example: 10000000000

{% enddocs %}

{% docs ez_stablecoins_supply_amount_in_cex %}

The amount of tokens held in centralized exchange addresses.

Example: 300000000

{% enddocs %}

{% docs ez_stablecoins_supply_amount_in_bridges %}

The amount of tokens held in bridge vaults and contracts.

Example: 50000000

{% enddocs %}

{% docs ez_stablecoins_supply_amount_in_dex_liquidity_pools %}

The amount of tokens deposited in decentralized exchange liquidity pools.

Example: 200000000

{% enddocs %}

{% docs ez_stablecoins_supply_amount_in_lending_pools %}

The amount of tokens deposited in lending protocol pools.

Example: 150000000

{% enddocs %}

{% docs ez_stablecoins_supply_amount_in_contracts %}

The amount of tokens held in all contracts (including other categorized contracts).

Example: 100000000

{% enddocs %}