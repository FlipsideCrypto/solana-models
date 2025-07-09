{% docs block_timestamp %}
The UTC timestamp when the block containing this transaction was confirmed on the Solana blockchain. Used to order transactions chronologically and analyze activity over time. Format: YYYY-MM-DD HH:MI:SS. This value is derived from the block metadata and is critical for time-based analytics, such as transaction volume by hour or day.

**Example:**
- "2024-05-01 12:34:56"

**Business Context:**
- Enables time-series analysis, latency measurement, and event correlation.
- Used for compliance, reporting, and monitoring network activity.

**Relationships:**
- Shared across all transactions in the same block (see 'block_id').
{% enddocs %}