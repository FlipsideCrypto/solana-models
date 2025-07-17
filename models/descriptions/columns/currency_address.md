{% docs currency_address %}

The address of the token used to pay for the NFT transaction. This field identifies which cryptocurrency or token was used as payment, enabling analysis of payment preferences and market dynamics.

- **Data type:** STRING (Solana address, e.g., 'So11111111111111111111111111111111111111112' for SOL, 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' for USDC)
- **Business context:** Used to identify the payment asset for NFT sales and mints, analyze payment trends, and support cross-token analytics.
- **Analytics use cases:** Payment token flow analysis, market share by token, cross-currency price comparisons, and filtering NFT sales by payment asset.
- **Example:** 'So11111111111111111111111111111111111111112' (SOL)

{% enddocs %} 