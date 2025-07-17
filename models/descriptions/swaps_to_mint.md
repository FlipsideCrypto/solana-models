{% docs swaps_to_mint %}

The mint address of the token being received or swapped to in the transaction. This is a unique identifier for the SPL token or SOL.

- **Data type:** STRING (base58 Solana mint address)
- **Business context:** Used to identify the destination asset in the swap, filter by token, and analyze token-specific inflows.
- **Analytics use cases:** Token inflow analysis, asset demand studies, and DEX routing analytics.
- **Example:** SOL: `So11111111111111111111111111111111111111112`, USDT: `BQvQ8...`

{% enddocs %}