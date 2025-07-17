{% docs swaps_from_amt %}

The total amount of the token sent in to initiate the swap, as recorded on-chain. This value is already decimal adjusted according to the token's standard decimals, and represents the human-readable amount.

- **Data type:** NUMBER (float or integer, depending on token)
- **Business context:** Used to calculate swap volume, analyze liquidity flows, and measure user activity.
- **Analytics use cases:** Volume analysis, liquidity tracking, slippage calculations, and DEX protocol comparisons.
- **Example:** For SOL, a value of `2.5` means 2.5 SOL. For USDC, a value of `100.5` means 100.5 USDC.

{% enddocs %}