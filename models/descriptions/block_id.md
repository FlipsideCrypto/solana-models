{% docs block_id %}
A unique identifier for the block in which this transaction was included on the Solana blockchain. Typically a sequential integer or hash, depending on the data source. Used to group transactions by block and analyze block-level activity.

**Example:**
- 123456789

**Business Context:**
- Supports block-level analytics, such as block production rate and transaction throughput.
- Useful for tracing transaction inclusion and block explorer integrations.

**Relationships:**
- All transactions with the same 'block_id' share the same 'block_timestamp'.
{% enddocs %}