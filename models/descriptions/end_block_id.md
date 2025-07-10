{% docs end_block_id %}

The block ID (slot number) where the ownership period for the token account ends (exclusive). A null value indicates the current owner. Used to determine the time range during which a specific owner controlled the account.

**Data type:** Integer (slot number) or null
**Example:**
- `123456999` (ownership ended at this block)
- `null` (current owner)

**Business Context:**
- Enables historical attribution of token balances and transfers to the correct owner.
- Supports analytics on ownership changes, user activity, and protocol events.

{% enddocs %} 