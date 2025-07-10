{% docs event_index %}
The position of the event (instruction) within the list of instructions for a given Solana transaction. Used to order and reference events within a transaction. Indexing starts at 0 for the first event.

**Example:**
- 0
- 3

**Business Context:**
- Enables precise identification and ordering of events within a transaction, which is critical for reconstructing transaction flows and analyzing protocol behavior.
- Used to join or filter event-level data, especially when multiple events occur in a single transaction.

**Relationships:**
- Used with 'block_id', 'tx_id', and (optionally) 'inner_index' for unique event identification.
{% enddocs %}