{% docs swaps_swapper %}

The Solana address (base58-encoded string) that initiated the swap transaction. This field identifies the user or contract responsible for the swap, enabling wallet-level analytics, behavioral segmentation, and user journey analysis.

- **Data type:** STRING (base58 Solana address, e.g., `4Nd1mYw4r...`)
- **Business context:** Used to group swaps by user, analyze trading patterns, and track protocol adoption.
- **Analytics use cases:** User segmentation, whale tracking, protocol adoption analysis, and cross-table joins with other user-activity models.
- **Example:** `4Nd1mYw4r...`

{% enddocs %}