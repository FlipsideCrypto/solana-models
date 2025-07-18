{% docs prior_voters %}

Prior voters for the vote account. This field tracks the historical list of voters that have been associated with this vote account, enabling voter history analysis and account ownership tracking.

**Data type:** ARRAY (voter addresses)
**Business context:** Used to track vote account ownership history, analyze voter transitions, and understand account control changes.
**Analytics use cases:** Ownership history tracking, voter transition analysis, and account control change monitoring.
**Example:** ['PreviousVoter1', 'PreviousVoter2'], ['OldVoterAddress']

{% enddocs %} 