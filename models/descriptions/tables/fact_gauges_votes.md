{% docs fact_gauges_votes %}

## Description
This table tracks gauge voting events, particularly for Marinade Finance's gauge system. Gauge voting allows users to allocate rewards to different liquidity pools or protocols, providing a mechanism for decentralized reward distribution and liquidity incentivization.

## Key Use Cases
- Analyze gauge voting patterns and reward allocation preferences
- Track reward distribution across different protocols and pools
- Study liquidity incentivization strategies and effectiveness
- Monitor gauge voting participation and user behavior
- Support DeFi analytics and reward optimization

## Important Relationships
- Links to `gov.fact_gov_actions` through governance context for comprehensive governance analysis
- Connects to `gov.fact_gauges_creates` for gauge creation lifecycle
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- Provides gauge context for DeFi analytics

## Commonly-used Fields
- `block_timestamp`: Timestamp when the gauge vote was cast
- `tx_id`: Unique transaction identifier for the gauge vote
- `voter`: Address of the account that cast the gauge vote
- `gauge`: Address of the gauge being voted on
- `vote_weight`: Weight of the vote for reward allocation
- `program_name`: Name of the gauge program (e.g., 'marinade')

{% enddocs %} 