{% docs amount %}

The amount of the asset transferred in the event. For native SOL, this is decimal adjusted and is not in Lamports. For SPL tokens, this is decimal adjusted according to the token's mint. Represents the value moved from sender to recipient in a single transfer event.

**Data type:** Numeric (integer for lamports, decimal for tokens)
**Example:**
- USDC: `50.00` (represents 50 USDC tokens)

**Business Context:**
- Used to analyze transaction volumes, user activity, and protocol flows.
- Supports aggregation of asset movement for analytics and reporting.

{% enddocs %}