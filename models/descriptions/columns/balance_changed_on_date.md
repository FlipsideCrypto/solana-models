{% docs balance_changed_on_date %}

Boolean flag indicating whether the balance actually changed on this specific date. TRUE means there was a balance-changing transaction on this date, FALSE means the balance was forward-filled from a previous date to maintain a complete daily time series.

**Data type:** BOOLEAN
**Business context:** Distinguishes between days with actual balance activity versus days where balances are carried forward. Critical for understanding account activity patterns and data completeness.
**Analytics use cases:** Account activity tracking, identifying active trading days, filtering for actual balance changes, and understanding transaction frequency patterns.
**Example:** TRUE (balance changed on this date), FALSE (balance carried forward from previous day)

{% enddocs %}
