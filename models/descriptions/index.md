{% docs index %}

The position of the transfer event within the list of events for a given Solana transaction. Used to order and reference transfers within a transaction. Indexing starts at 0 for the first event.

**Data type:** Integer
**Example:**
- 0 (first transfer in the transaction)
- 2 (third transfer in the transaction)

**Business Context:**
- Enables reconstruction of transfer order and analysis of intra-transaction asset movement.
- Used to join, filter, or segment data for protocol analytics, error tracing, and event sequencing.

**Relationships:**
- Used with 'block_id', 'tx_id', and (optionally) 'inner_index' for unique identification of transfer events.

{% enddocs %}