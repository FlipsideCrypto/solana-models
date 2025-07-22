{% docs votes %}

Votes cast by the vote account during the epoch. This field tracks the voting activity of the vote account within the current epoch, enabling participation analysis and consensus contribution measurement.

**Data type:** ARRAY (vote data)
**Business context:** Used to track vote account participation, analyze voting patterns, and measure consensus contribution.
**Analytics use cases:** Participation tracking, voting pattern analysis, and consensus contribution measurement.
**Example:** [{'slot': 123456789, 'confirmation_count': 32}, {'slot': 123456790, 'confirmation_count': 31}]

{% enddocs %} 