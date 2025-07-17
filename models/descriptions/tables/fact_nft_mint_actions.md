{% docs fact_nft_mint_actions %}

## Description
This table captures detailed minting events for NFTs on Solana, providing granular information about each mint action including the minting authority, amount, and technical details. It tracks the actual minting operations that occur when NFTs are created, burned, or modified, offering insights into the underlying mechanics of NFT lifecycle management and supporting technical analysis of NFT operations.

## Key Use Cases
- Track which addresses have authority to mint NFTs and their activity patterns
- Monitor minting and burning activities that affect NFT supply
- Study how different programs interact with NFT mints
- Analyze the technical details of minting operations for debugging and optimization
- Verify that minting operations follow proper NFT standards

## Important Relationships
- Links to `nft.dim_nft_metadata` via `mint` address to provide metadata context
- Connects to `nft.fact_nft_mints` through `mint` address for comprehensive mint tracking
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- `index` and `inner_index` provide ordering within transaction instructions

## Commonly-used Fields
- `block_timestamp`: Timestamp when the mint action was processed on Solana
- `tx_id`: Unique transaction identifier for the mint action
- `mint`: The unique mint address of the NFT being acted upon
- `mint_authority`: Address of the account with authority to perform the mint action
- `mint_amount`: Amount of tokens minted in the action (typically 1 for NFTs)
- `event_type`: Type of mint action (e.g., 'mint', 'burn', 'transfer')
- `mint_standard_type`: NFT standard being used (e.g., 'metaplex', 'candy_machine')

{% enddocs %}