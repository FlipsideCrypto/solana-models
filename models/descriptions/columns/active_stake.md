{% docs active_stake %}

The amount of SOL actively staked in this stake account that is participating in consensus. This represents the portion of the stake account balance that is currently active and contributing to validator voting power. If the current epoch is greater than the deactivation_epoch, this value will be 0 as the stake is no longer active.

**Data type:** DECIMAL (SOL amount)
**Business context:** Essential for calculating validator total stake, delegation analysis, and understanding stake account lifecycle states.
**Analytics use cases:** Validator stake distribution analysis, delegation patterns, stake account activation/deactivation tracking, and network security assessment.
**Example:** 5000.0 (5,000 SOL), 0 (deactivated stake)

{% enddocs %} 