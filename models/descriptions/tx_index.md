{% docs tx_index %}
The position of this transaction within its containing block, starting from zero. Used to order transactions within a block and analyze intra-block activity.

**Example:**
- 0
- 15

**Business Context:**
- Useful for reconstructing block order and analyzing transaction sequencing.
- Can be used to identify priority transactions or block congestion.

**Relationships:**
- Used with 'block_id' and 'block_timestamp' for full ordering.
{% enddocs %}