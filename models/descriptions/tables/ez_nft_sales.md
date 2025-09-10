{% docs ez_nft_sales %}

## Description
This table provides a unified view of NFT sales across multiple Solana marketplaces, including Magic Eden, Hadeswap, Exchange Art, TensorSwap, and others. It consolidates sales data from various marketplace sources and enriches it with NFT metadata, USD pricing, and marketplace information. The table supports both standard NFTs and compressed NFTs (cNFTs), with USD prices available from December 16, 2021 onwards, enabling comprehensive cross-marketplace analysis.

## Key Use Cases
- Compare sales volumes and trends across different NFT marketplaces
- Monitor NFT price movements and market performance over time
- Analyze sales performance of specific NFT collections and individual assets
- Study buyer and seller patterns across different marketplaces and time periods
- Track creator royalties and marketplace fees from NFT sales
- Identify trending collections and high-value NFT transactions

## Important Relationships
- Links to `nft.dim_nft_metadata` via `mint` address to provide rich NFT context
- Connects to `price.ez_prices_hourly` for USD price conversion and `price.ez_asset_metadata` for currency symbols
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- Includes specialized fields (`tree_authority`, `merkle_tree`, `leaf_index`) for cNFT transactions

## Commonly-used Fields
- `block_timestamp`: Timestamp when the sale transaction was processed on Solana
- `tx_id`: Unique transaction identifier for the sale
- `buyer_address`: Address of the account that purchased the NFT
- `seller_address`: Address of the account that sold the NFT
- `mint`: The unique mint address of the NFT being sold
- `price`: Sale amount in the native currency (typically SOL)
- `price_usd`: Sale amount converted to USD for cross-marketplace comparison
- `marketplace`: Name of the marketplace where the sale occurred
- `currency_address`: Address of the token used for payment
- `currency_symbol`: Symbol of the payment token (e.g., SOL, USDC)
- `is_compressed`: Boolean indicating if the NFT is a compressed NFT (cNFT)
- `nft_name`: Human-readable name of the NFT from metadata
- `nft_collection_name`: Name of the collection the NFT belongs to

## Sample Queries

### Daily NFT marketplace volume and activity
```sql
SELECT 
    DATE_TRUNC('day', block_timestamp) AS date,
    marketplace,
    COUNT(*) AS total_sales,
    COUNT(DISTINCT buyer_address) AS unique_buyers,
    COUNT(DISTINCT seller_address) AS unique_sellers,
    COUNT(DISTINCT mint) AS unique_nfts_traded,
    SUM(price) AS total_volume_native,
    SUM(price_usd) AS total_volume_usd,
    AVG(price_usd) AS avg_sale_price_usd,
    MAX(price_usd) AS highest_sale_usd
FROM solana.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 30
    AND price_usd > 0
GROUP BY 1, 2
ORDER BY 1 DESC, total_volume_usd DESC;
```

### Top NFT collections by volume
```sql
SELECT 
    nft_collection_name,
    COUNT(*) AS total_sales,
    COUNT(DISTINCT mint) AS unique_nfts_sold,
    COUNT(DISTINCT buyer_address) AS unique_buyers,
    SUM(price_usd) AS total_volume_usd,
    AVG(price_usd) AS avg_price_usd,
    MIN(price_usd) AS floor_price_usd,
    MAX(price_usd) AS ceiling_price_usd,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_usd) AS median_price_usd
FROM solana.nft.ez_nft_sales
WHERE block_timestamp >= CURRENT_DATE - 7
    AND price_usd > 0
    AND nft_collection_name IS NOT NULL
GROUP BY 1
HAVING total_sales >= 10
ORDER BY total_volume_usd DESC
LIMIT 50;
```

### NFT flipping activity (bought and sold within short timeframe)
```sql
WITH nft_transactions AS (
    SELECT 
        mint,
        buyer_address AS owner,
        seller_address AS previous_owner,
        block_timestamp AS purchase_time,
        price_usd AS purchase_price,
        tx_id
    FROM solana.nft.ez_nft_sales
    WHERE block_timestamp >= CURRENT_DATE - 7
        AND price_usd > 0
),
flips AS (
    SELECT 
        t1.mint,
        t1.owner AS flipper,
        t1.purchase_time AS buy_time,
        t2.purchase_time AS sell_time,
        t1.purchase_price AS buy_price,
        t2.purchase_price AS sell_price,
        t2.purchase_price - t1.purchase_price AS profit_usd,
        (t2.purchase_price - t1.purchase_price) / NULLIF(t1.purchase_price, 0) * 100 AS profit_pct,
        DATEDIFF('hour', t1.purchase_time, t2.purchase_time) AS hold_time_hours
    FROM nft_transactions t1
    INNER JOIN nft_transactions t2
        ON t1.mint = t2.mint
        AND t1.owner = t2.previous_owner
        AND t2.purchase_time > t1.purchase_time
        AND t2.purchase_time <= t1.purchase_time + INTERVAL '3 days'
)
SELECT 
    mint,
    flipper,
    buy_time,
    sell_time,
    hold_time_hours,
    buy_price,
    sell_price,
    profit_usd,
    profit_pct
FROM flips
WHERE ABS(profit_usd) > 10
ORDER BY profit_usd DESC
LIMIT 100;
```

{% enddocs %} 