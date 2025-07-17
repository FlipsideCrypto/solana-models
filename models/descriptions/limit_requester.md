{% docs limit_requester %}

The original address that requested the limit order swap. This field identifies the user who initiated the limit order, enabling user-level limit order analytics.

- **Data type:** STRING (base58 Solana address, NULL if not a limit order swap)
- **Business context:** Used to track limit order users, analyze limit order user behavior, and measure limit order feature adoption at the user level.
- **Analytics use cases:** Limit order user segmentation, user behavior analysis, and Jupiter product adoption studies.
- **Example:** '4Nd1mYw4r...'

{% enddocs %} 