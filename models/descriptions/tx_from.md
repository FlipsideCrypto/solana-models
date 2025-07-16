{% docs tx_from %}

The base58-encoded wallet address that initiates the transfer event. For native SOL and SPL token transfers, this is the sender's address. Used to attribute outgoing asset movement to specific users or programs.

**Data type:** String (base58 address)
**Example:**
- `7GgkQ2...`

**Business Context:**
- Enables analysis of asset outflows, user activity, and protocol interactions.

{% enddocs %}