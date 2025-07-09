{% docs program_id %}
The unique public key (base58-encoded address) of a Solana program. This field identifies the on-chain program (smart contract) responsible for processing instructions, emitting events, or managing accounts. Used throughout Solana analytics models—including events, transactions, IDLs, and program activity tables—to join, filter, and analyze program-level data.

**Example:**
- "4Nd1mY..."
- "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

**Business Context:**
- Used as a join key for program activity, deployments, events, and interface changes.
- Supports segmentation of activity by protocol, DEX, NFT marketplace, or other on-chain application.

{% enddocs %}