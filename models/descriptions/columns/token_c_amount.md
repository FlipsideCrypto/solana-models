{% docs token_c_amount %}

The amount of the third token involved in the liquidity pool action, already decimal adjusted according to the token's standard decimals.

- **Data type:** NUMBER (float or integer, depending on token)
- **Business context:** Used to track the third token's liquidity flows, analyze multi-token pool activity, and measure token-specific liquidity provision.
- **Analytics use cases:** Multi-token liquidity analysis, token-specific flow tracking, and cross-token liquidity studies.
- **Example:** For SOL, a value of `5.0` means 5 SOL.

{% enddocs %} 