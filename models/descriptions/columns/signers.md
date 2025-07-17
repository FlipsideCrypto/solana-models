{% docs signers %}
An array of account addresses that have signed the transaction, authorizing its execution on the Solana blockchain. Each signer is a base58-encoded public key. Signers are required for transaction validity and may include the fee payer and other accounts with write permissions. The order of signers matches their appearance in the transaction's account_keys array. For more details, see: https://docs.solana.com/developing/programming-model/accounts#signers

**Example:**
- ["3N5k...", "7G8h..."]

**Business Context:**
- Used to verify transaction authorization and trace which accounts initiated or approved actions.
- Important for analytics on user activity, multisig wallets, and program interactions.

**Relationships:**
- Subset of the 'account_keys' array.
- The first signer is typically the fee payer.
{% enddocs %}