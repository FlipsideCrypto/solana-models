{% docs tx_id %}

The unique transaction signature (hash) for each transaction on the Solana blockchain. This field is a base58-encoded string, typically 88 characters in length, and serves as the primary identifier for transactions across all Solana data models. Used to join transaction data with related tables (blocks, events, transfers, logs, decoded instructions) and to trace the full lifecycle and effects of a transaction. Essential for transaction-level analytics, debugging, and cross-referencing with block explorers or Solana APIs.

**Example:**
- `5Nf6Q2k6v1Qw2k3v4Qw5Nf6Q2k6v1Qw2k3v4Qw5Nf6Q2k6v1Qw2k3v4Qw5Nf6Q2k6v1Qw2k3v4Qw`

**Business Context:**
- Enables precise tracking, auditing, and attribution of on-chain activity
- Used for linking transactions to events, logs, and protocol actions
- Critical for compliance, monitoring, and analytics workflows

{% enddocs %}