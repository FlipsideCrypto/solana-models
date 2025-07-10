{% docs instruction_program_id %}
The unique public key (base58-encoded address) of the Solana program that invoked this inner instruction (Cross-Program Invocation, CPI). This field identifies the calling program.

**Example:**
- "4Nd1mY..." (caller)
- "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" (callee)

**Business Context:**
- Enables tracing of program-to-program calls and composability patterns in Solana transactions.
- Used to analyze protocol integrations, nested execution flows, and the origin of on-chain actions.

**Relationships:**
- Used with 'program_id', 'tx_id', and instruction indices to reconstruct full CPI call stacks.
- 'instruction_program_id' is the caller; 'program_id' is the callee for the inner instruction.
{% enddocs %} 