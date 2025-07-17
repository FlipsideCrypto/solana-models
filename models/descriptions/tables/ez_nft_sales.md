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

{% enddocs %} 