{% docs fact_swaps %}

## Description
This table contains all swap transactions across major Solana DEXes, including curated DEX protocols (Raydium, Orca, Meteora, Bonkswap, etc.) and standalone swaps. It provides visibility into every swap that uses the curated programs, whether it is a direct swap through a DEX or a swap that is a step in a Jupiter route. This table is foundational for analyzing swap activity, comparing swap volume, and understanding user intent across the Solana DeFi ecosystem.

## Key Use Cases
- Analyze all swaps for every supported DEX program (Raydium, Orca, Meteora, Bonkswap, etc.)
- Calculate real swap volume and compare swap activity across DEXes
- Compare activity across different curated swap programs
- Analyze user intent behind Jupiter swaps (with reference to jupiter summary/inner tables)
- Support for user-level metrics, buy/sell volume, and intent distinction

## Important Relationships
- Use with `defi.ez_dex_swaps` for enhanced analytics and USD pricing
- Use with `defi.fact_swaps_jupiter_inner` to analyze routed swaps and full Jupiter paths
- Use with `defi.fact_swaps_jupiter_summary` for user-level intent and summarized Jupiter swaps
- Example queries: buy/sell volume for a token, swap metrics by DEX protocol, compare activity across routed/direct swaps

## Commonly-used Fields
- `block_timestamp`, `swapper`, `swap_from_mint`, `swap_to_mint`, `swap_from_amount`, `swap_to_amount`, `swap_program`

{% enddocs %} 