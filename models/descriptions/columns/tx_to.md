{% docs tx_to %}

The base58-encoded wallet address that receives the asset in a transfer event. For native SOL and SPL token transfers, this is the recipient's address. Used to attribute incoming asset movement to specific users or programs.

**Data type:** String (base58 address)
**Example:**
- `9xQeWv...`

**Business Context:**
- Enables analysis of asset inflows, user activity, and protocol interactions.

{% enddocs %}