{% docs dca_requester %}

The original address that requested the DCA (Dollar Cost Averaging) swap. This field identifies the user who initiated the DCA order, enabling user-level DCA analytics.

- **Data type:** STRING (base58 Solana address, NULL if not a DCA swap)
- **Business context:** Used to track DCA users, analyze DCA user behavior, and measure DCA feature adoption at the user level.
- **Analytics use cases:** DCA user segmentation, user behavior analysis, and Jupiter product adoption studies.
- **Example:** '4Nd1mYw4r...'

{% enddocs %} 