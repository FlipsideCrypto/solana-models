{% docs owner %}

The base58-encoded address of the owner controlling the token account during the specified block range. Ownership may change over time due to transfers or account reassignment. Used to attribute token balances and transfers to the correct user or program at any point in time.

**Data type:** String (base58 address)
**Example:**
- `7GgkQ2...`

**Business Context:**
- Enables historical tracking of token account ownership.
- Supports analytics on user activity, protocol attribution, and token flows.

{% enddocs %} 