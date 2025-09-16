{% docs active_stake_validator %}

The total active stake in lamports delegated to the validator. This field represents the aggregate amount of stake currently active and participating in consensus for the validator. To convert from lamports to SOL, divide by pow (10,9) since 1 SOL = 1000000000 lamports.

**Data type:** DECIMAL (lamports, raw amount)
**Business context:** Used to analyze validator stake distribution, delegation patterns, and network security assessment. Essential for calculating validator market share and network decentralization metrics.
**Analytics use cases:** Validator stake distribution analysis, delegation patterns, network security assessment, and validator ranking by total stake.
**Example:** 5000000000000 (5,000 SOL), 1500000000000 (1,500 SOL)

{% enddocs %} 