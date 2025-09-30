{% docs last_balance_change %}

The date when this account-mint combination last had an actual balance change. This field tracks the most recent date when the balance was modified, which may be earlier than the balance_date due to forward-filling of balances on days with no activity.

**Data type:** DATE
**Business context:** Used to identify active vs. inactive accounts, understand balance change frequency, and distinguish between actual balance changes and forward-filled values.
**Analytics use cases:** Account activity analysis, dormant account identification, balance change frequency tracking, and data freshness assessment.
**Example:** 2024-01-10 (when balance_date is 2024-01-15, indicating no changes for 5 days)

{% enddocs %}
