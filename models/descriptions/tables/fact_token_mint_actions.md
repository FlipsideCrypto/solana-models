{% docs fact_token_mint_actions %}

## Description
This table contains information on all token mint events on the Solana blockchain. It tracks the creation of new tokens across various token standards (SPL, Metaplex, etc.), capturing mint authority actions, token account destinations, and mint amounts. Each row represents a single token mint event, supporting analytics on token creation, supply dynamics, and token standard usage.

## Key Use Cases
- Analyze token creation patterns and supply dynamics
- Track mint authority activity and token issuance
- Study token standard adoption (SPL, Metaplex, etc.)
- Monitor new token launches and initial distributions
- Support analytics on token economics and supply management

## Important Relationships
- Closely related to `defi.fact_token_burn_actions` (for burn events), `core.ez_transfers` (for token movements), and `core.fact_token_balances` (for balance changes)
- Use `defi.fact_token_burn_actions` to analyze token destruction and supply reduction
- Use `core.ez_transfers` to track token movements after minting
- Use `core.fact_token_balances` to analyze balance changes from minting
- Joins with `core.fact_blocks` for block context and `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and mint activity analysis
- `mint`, `mint_authority`, `token_account`: For token and authority identification
- `mint_amount`: For supply and value analytics
- `mint_standard_type`: For token standard analysis
- `succeeded`: For transaction success analysis

{% enddocs %} 