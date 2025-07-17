{% docs start_block_id %}

The block ID (slot number) where the ownership period for the token account begins (inclusive). Used to determine the time range during which a specific owner controlled the account.

**Data type:** Integer (slot number)
**Example:**
- `123456789`

**Business Context:**
- Enables historical attribution of token balances and transfers to the correct owner.
- Supports analytics on ownership changes, user activity, and protocol events.

{% enddocs %} 