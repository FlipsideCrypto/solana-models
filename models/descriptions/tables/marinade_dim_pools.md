{% docs marinade_dim_pools %}

## Description
This table contains information on liquidity pools that involve MNDE or MSOL tokens as one of the liquidity pairs across multiple DeFi protocols including Raydium, Orca, and Meteora. It serves as a dimensional table providing comprehensive pool metadata, token information, and platform categorization for Marinade Finance ecosystem analysis.

## Key Use Cases
- Analyze liquidity pool distribution across different DeFi protocols
- Track MSOL and MNDE token liquidity availability and depth
- Monitor pool initialization and creation patterns
- Support cross-protocol liquidity analysis and comparison
- Enable pool performance analysis and token pair tracking

## Important Relationships
- Links to `marinade.ez_liquidity_pool_actions` through pool addresses for action analysis
- Connects to `marinade.ez_swaps` through pool addresses for swap activity tracking
- References `price.ez_asset_metadata` for token symbol and metadata information
- Provides pool context for Marinade Finance ecosystem analytics

## Commonly-used Fields
- `pool_address`: Essential for linking to pool actions and swaps
- `pool_name`: Key for human-readable pool identification and analysis
- `token_a_mint` and `token_b_mint`: Critical for token pair analysis and filtering
- `platform`: Important for cross-protocol comparison and platform-specific analysis
- `is_msol_pool` and `is_mnde_pool`: Key for Marinade-specific pool filtering and analysis

{% enddocs %} 