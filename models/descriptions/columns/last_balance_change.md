{% docs last_balance_change %}

The date when this account last had an actual balance change that resulted in a positive balance. This field tracks the most recent date when the balance was modified to a positive amount, which may be earlier than the balance_date due to forward-filling of balances on days with no activity. Note that accounts with zero balances are excluded from daily balance tables.

**Data type:** DATE
**Business context:** Used to identify active vs. inactive accounts, understand balance change frequency, and distinguish between actual balance changes and forward-filled values. Only accounts with positive balances are tracked.
**Analytics use cases:** Account activity analysis, dormant account identification, balance change frequency tracking, data freshness assessment, and identifying when accounts last held positive balances.
**Example:** 2024-01-10 (when balance_date is 2024-01-15, indicating no changes for 5 days)

{% enddocs %}
