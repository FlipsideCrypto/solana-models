{% docs inner_index %}
The position of the inner instruction or event within the list of inner instructions for a given Solana transaction. Used to order and reference nested (CPI) instructions. Indexing starts at 0 for the first inner instruction.

**Example:**
- 0
- 2

**Business Context:**
- Enables precise identification and ordering of nested program calls (Cross-Program Invocations) within a transaction.
- Critical for analyzing composability, protocol integrations, and the full execution path of complex transactions.

**Relationships:**
- Used with 'block_id', 'tx_id', 'instruction_index', and 'program_id' for unique identification of inner events.
{% enddocs %}