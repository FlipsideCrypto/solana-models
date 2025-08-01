{% docs burn_amount %}

The amount of tokens being burned in the transaction, denominated in the token's smallest unit (e.g., lamports for SOL, or the base unit for SPL tokens). This field enables token supply analysis and burn tracking.

- **Data type:** NUMBER (integer, token's smallest unit)
- **Business context:** Used to track token burns, analyze token supply changes, and measure deflationary pressure.
- **Analytics use cases:** Token supply analysis, burn rate tracking, and deflationary token studies.
- **Example:** For SOL, 1 SOL = 1,000,000,000 lamports; a value of `1000000000` means 1 SOL burned.

{% enddocs %}