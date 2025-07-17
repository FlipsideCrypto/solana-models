{% docs fact_token_burn_actions %}

## Description
This table contains information on all token burn events on the Solana blockchain. It tracks the destruction of tokens across various token standards (SPL, Metaplex, etc.), capturing burn authority actions, token account sources, and burn amounts. Each row represents a single token burn event, supporting analytics on token destruction, supply reduction, and token standard usage.

## Key Use Cases
- Analyze token destruction patterns and supply reduction
- Track burn authority activity and token destruction
- Study token standard adoption (SPL, Metaplex, etc.)
- Monitor token supply management and deflationary mechanisms
- Support analytics on token economics and supply dynamics

## Important Relationships
- Closely related to `defi.fact_token_mint_actions` (for mint events), `core.fact_transfers` (for token movements), and `core.fact_token_balances` (for balance changes)
- Use `defi.fact_token_mint_actions` to analyze token creation and supply increase
- Use `core.fact_transfers` to track token movements before burning
- Use `core.fact_token_balances` to analyze balance changes from burning
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and burn activity analysis
- `mint`, `burn_authority`, `token_account`: For token and authority identification
- `burn_amount`: For supply and value analytics
- `mint_standard_type`: For token standard analysis
- `succeeded`: For transaction success analysis

{% enddocs %} 