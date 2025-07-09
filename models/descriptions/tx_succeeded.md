{% docs tx_succeeded %}
Boolean flag indicating whether the transaction was successfully executed and confirmed on the Solana blockchain. A value of TRUE means the transaction was processed without errors; FALSE indicates failure due to program errors, insufficient funds, or other issues.

**Example:**
- true
- false

**Business Context:**
- Used to filter for successful transactions in analytics and reporting.
- Important for error analysis, user experience, and program debugging.

{% enddocs %}