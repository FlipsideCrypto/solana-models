{% docs lockup %}

Lockup information specifying when tokens can be withdrawn from the stake account. This field contains the lockup conditions including epoch, timestamp, and custodian restrictions, enabling withdrawal analysis and lockup tracking.

**Data type:** OBJECT (lockup data)
**Business context:** Used to track withdrawal restrictions, analyze lockup conditions, and monitor stake account liquidity.
**Analytics use cases:** Withdrawal restriction tracking, lockup condition analysis, and liquidity monitoring.
**Example:** {'epoch': 123, 'unix_timestamp': 1642233600, 'custodian': 'CustodianAddress'}

{% enddocs %} 