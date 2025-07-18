{% docs post_token_balances %}

List of post-transaction token balances for different token accounts. This field captures the token balances of all token accounts involved in the transaction after execution, enabling token balance change analysis.

**Data type:** ARRAY (token balance objects)
**Business context:** Used to track token balance changes, analyze token movements, and measure token transaction impact.
**Analytics use cases:** Token balance change analysis, token movement tracking, and token transaction impact measurement.
**Example:** [{'mint': 'TokenMintAddress', 'amount': 900}, {'mint': 'AnotherToken', 'amount': 600}]

{% enddocs %} 