{% docs fact_gauges_creates %}

## Description
This table tracks gauge creation events, particularly for Marinade Finance's gauge system. Gauge creation establishes new reward distribution mechanisms for liquidity pools or protocols, enabling decentralized control over reward allocation and liquidity incentivization strategies.

## Key Use Cases
- Track gauge creation patterns and new reward distribution mechanisms
- Analyze gauge creation frequency and protocol adoption
- Study gauge lifecycle from creation to voting and reward distribution
- Monitor DeFi protocol reward strategies and incentivization
- Support gauge analytics and reward optimization

## Important Relationships
- Links to `gov.fact_gauges_votes` through `gauge` address for complete gauge lifecycle
- Connects to `gov.fact_gov_actions` for governance action analysis
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- Provides gauge context for DeFi analytics

## Commonly-used Fields
- `block_timestamp`: Timestamp when the gauge was created
- `tx_id`: Unique transaction identifier for the gauge creation
- `gauge`: Address of the created gauge
- `creator`: Address of the account that created the gauge
- `program_name`: Name of the gauge program (e.g., 'marinade')

{% enddocs %} 