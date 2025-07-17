{% docs fact_nft_burn_actions %}

## Description
This table tracks all NFT burning events on Solana, capturing when NFTs are permanently destroyed or removed from circulation. It provides detailed information about burn operations including the burning authority, amount burned, and technical transaction details. Burning is a critical mechanism for NFT supply management and can occur for various reasons including marketplace delisting, collection management, or user-initiated destruction, supporting comprehensive NFT lifecycle analysis.

## Key Use Cases
- Track NFT burning patterns and their impact on collection supply
- Monitor burning activities related to marketplace delisting and cleanup operations
- Analyze which addresses have authority to burn NFTs and their usage patterns
- Study how creators and collection managers use burning for supply control
- Complete the lifecycle view from minting to burning for comprehensive NFT analytics

## Important Relationships
- Links to `nft.dim_nft_metadata` via `mint` address to provide metadata context
- Connects to `nft.fact_nft_mints` through `mint` address to track complete NFT lifecycle
- References `nft.ez_nft_sales` to understand trading activity before burning
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- `index` and `inner_index` provide ordering within transaction instructions

## Commonly-used Fields
- `block_timestamp`: Timestamp when the burn action was processed on Solana
- `tx_id`: Unique transaction identifier for the burn action
- `mint`: The unique mint address of the NFT being burned
- `burn_authority`: Address of the account with authority to perform the burn action
- `burn_amount`: Amount of tokens burned in the action (typically 1 for NFTs)
- `event_type`: Type of burn action (e.g., 'burn', 'close_account')
- `mint_standard_type`: NFT standard being used (e.g., 'metaplex', 'candy_machine')

{% enddocs %} 