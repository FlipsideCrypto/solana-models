{% docs ez_transfers %}

## Description
This table contains transfer events for Solana and SPL tokens, including pre-parsed transfer amounts, USD value, token symbol, and verification status. Each row represents a single transfer event with enriched metadata that makes analytics easier compared to the raw `core.fact_transfers` table. The model includes both SOL and token transfers with complete pricing and token metadata, supporting comprehensive analytics on asset movement, wallet activity, payment flows, and large value transfers.

**This is the preferred table for transfer analytics** - it includes USD pricing, token symbols, verification status, and other enriched fields that are essential for most analytical use cases.

## Key Use Cases
- Track SOL and SPL token movements with USD valuations
- Analyze payment flows and wallet transaction histories
- Monitor large value transfers and whale activity with price context
- Build DeFi protocol volume analytics with accurate USD measurements
- Analyze token adoption and usage patterns using verified token metadata
- Create comprehensive asset movement dashboards with pricing data

## Important Relationships
- Enhanced version of `core.fact_transfers` with additional pricing and metadata fields
- Closely related to `core.fact_events` (for event context), `core.fact_events_inner` (for inner/CPI events), and `core.ez_events_decoded` (for decoded instruction details)
- Uses token price data from price tables to compute USD values
- Joins with `core.fact_transactions` for transaction context
- Essential for DeFi analytics when combined with protocol-specific tables like `defi.ez_dex_swaps` and `defi.ez_liquidity_pool_actions`

## Commonly-used Fields
- `block_timestamp`: For time-series and transfer sequencing analysis
- `tx_id`, `block_id`: For transaction and block context joins
- `tx_from`, `tx_to`: For sender and recipient analysis and wallet tracking
- `amount`, `amount_usd`: For value analysis in both token units and USD
- `mint`, `symbol`: For token-specific analytics and filtering
- `token_is_verified`: For filtering to verified/trusted tokens
- `signer`: For transaction initiator analysis

## Sample Queries

### Daily transfer volume and metrics
```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    COUNT(*) AS transfer_count,
    COUNT(DISTINCT tx_from) AS unique_senders,
    COUNT(DISTINCT tx_to) AS unique_receivers,
    COUNT(DISTINCT mint) AS unique_tokens,
    SUM(amount_usd) AS total_volume_usd,
    AVG(amount_usd) AS avg_transfer_usd,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount_usd) AS median_transfer_usd
FROM solana.core.ez_transfers
WHERE block_timestamp >= CURRENT_DATE - 30
    AND amount_usd > 0
GROUP BY 1
ORDER BY 1 DESC;
```

### Top token transfer routes
```sql
SELECT 
    tx_from,
    tx_to,
    symbol,
    mint,
    COUNT(*) AS transfer_count,
    SUM(amount) AS total_amount,
    SUM(amount_usd) AS total_usd,
    AVG(amount_usd) AS avg_transfer_usd
FROM solana.core.ez_transfers
WHERE block_timestamp >= CURRENT_DATE - 7
    AND amount_usd IS NOT NULL
GROUP BY 1, 2, 3, 4
HAVING total_usd > 10000
ORDER BY total_usd DESC
LIMIT 100;
```

### Token velocity analysis
```sql
SELECT 
    mint,
    symbol,
    COUNT(*) AS transfer_count,
    COUNT(DISTINCT tx_from) AS unique_senders,
    COUNT(DISTINCT tx_to) AS unique_receivers,
    SUM(amount_usd) AS total_volume_usd,
    COUNT(DISTINCT DATE(block_timestamp)) AS active_days,
    SUM(amount_usd) / NULLIF(COUNT(DISTINCT DATE(block_timestamp)), 0) AS daily_avg_volume_usd
FROM solana.core.ez_transfers
WHERE block_timestamp >= CURRENT_DATE - 7
    AND amount_usd > 0
GROUP BY 1, 2
HAVING total_volume_usd > 1000
ORDER BY total_volume_usd DESC;
```

{% enddocs %}
