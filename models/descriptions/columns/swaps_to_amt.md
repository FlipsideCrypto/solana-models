{% docs swaps_to_amt %}

The total amount of the token received from the swap, as recorded on-chain. This value is already decimal adjusted according to the token's standard decimals, and represents the human-readable amount.

- **Data type:** NUMBER (float or integer, depending on token)
- **Business context:** Used to measure swap output, analyze price impact, and track liquidity received.
- **Analytics use cases:** Output volume analysis, slippage and price impact studies, and DEX performance comparisons.
- **Example:** For USDC, a value of `1.0` means 1 USDC. For SOL, a value of `0.25` means 0.25 SOL.

{% enddocs %}