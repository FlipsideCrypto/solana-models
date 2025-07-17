{% docs swaps_from_mint %}

The mint address of the token being sent or swapped from in the transaction. This is a unique identifier for the SPL token or SOL.

- **Data type:** STRING (base58 Solana mint address)
- **Business context:** Used to identify the source asset in the swap, filter by token, and analyze token-specific activity.
- **Analytics use cases:** Token flow analysis, asset popularity studies, and cross-token comparisons.
- **Example:** SOL: `So11111111111111111111111111111111111111112`, USDC: `Es9vMFrzaCER...`

{% enddocs %}