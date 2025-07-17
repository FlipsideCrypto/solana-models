{% docs dim_nft_metadata %}

## Description
This table contains comprehensive metadata for all NFTs on Solana, including both standard NFTs and compressed NFTs (cNFTs). It serves as the authoritative source for NFT attributes such as names, collection information, creator details, and metadata URIs. The data is sourced from Helius API and enriched with collection information from Solscan, providing a complete view of NFT characteristics and relationships.

## Key Use Cases
- Query NFTs by name, collection, or creator to find specific assets
- Analyze NFT collections by grouping metadata and tracking creator royalties
- Provide rich metadata for NFT marketplaces and trading platforms
- Track creator performance and royalty distributions across NFT sales
- Verify NFT metadata completeness and quality for data integrity

## Important Relationships
- Links to `nft.ez_nft_sales` via `mint` address to provide metadata context for sales transactions
- Connects to `nft.fact_nft_mints` through `mint` address to enrich mint event data
- `collection_id` groups NFTs into collections, enabling collection-level analytics
- `creators` field contains creator addresses and royalty percentages for revenue tracking

## Commonly-used Fields
- `mint`: The unique mint address of the NFT (primary identifier)
- `nft_name`: Human-readable name of the NFT for display and search
- `nft_collection_name`: Name of the collection the NFT belongs to
- `collection_id`: Address identifier for the NFT collection
- `creators`: JSON array containing creator addresses and royalty percentages
- `authority`: Address with authority over the NFT mint (important for editions)
- `image_url`: Direct link to the NFT's image for display purposes
- `metadata_uri`: URI pointing to the complete metadata JSON for the NFT
- `metadata`: Parsed metadata object containing all NFT attributes and properties

{% enddocs %} 