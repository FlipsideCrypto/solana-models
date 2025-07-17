{% docs fact_nft_mints %}

## Description
This table tracks all NFT minting events on Solana, including both standard NFTs and compressed NFTs (cNFTs). It captures the initial creation of NFTs, including mint prices, purchasers, and program information. The data is sourced from various minting programs and provides a comprehensive view of NFT creation activity across the Solana ecosystem, supporting analysis of minting trends and creator economics.

## Key Use Cases
- Track NFT creation trends and identify popular minting periods
- Analyze which minting programs are most active and successful
- Study mint prices and their correlation with subsequent market performance
- Monitor revenue generated from initial NFT sales
- Understand the rate of new NFT creation and its impact on market dynamics
- Track the adoption and usage of compressed NFTs vs standard NFTs

## Important Relationships
- Links to `nft.dim_nft_metadata` via `mint` address to provide metadata context
- Connects to `nft.ez_nft_sales` through `mint` address to track post-mint trading activity
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- `program_id` identifies the specific minting program used for creation

## Commonly-used Fields
- `block_timestamp`: Timestamp when the mint transaction was processed on Solana
- `tx_id`: Unique transaction identifier for the mint event
- `purchaser`: Address of the account that purchased the NFT during minting
- `mint`: The unique mint address of the newly created NFT
- `mint_price`: Price paid to mint the NFT in the specified currency
- `mint_currency`: Address of the token used to pay for the mint
- `program_id`: Address of the minting program that created the NFT
- `is_compressed`: Boolean indicating if the minted NFT is a compressed NFT (cNFT)

{% enddocs %}