{% docs signers %}

List of accounts that signed the transaction. This field captures all wallet addresses that provided signatures for the transaction, enabling multi-signature analysis and transaction authority tracking.

**Data type:** ARRAY (list of Solana addresses)
**Business context:** Used to track transaction signers, analyze multi-signature patterns, and identify transaction authorities.
**Analytics use cases:** Multi-signature analysis, transaction authority tracking, and signer pattern studies.
**Example:** ['9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM', 'AnotherAddress...']

{% enddocs %}