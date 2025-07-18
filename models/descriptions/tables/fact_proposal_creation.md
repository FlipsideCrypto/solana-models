{% docs fact_proposal_creation %}

## Description
This table tracks governance proposal creation events across different platforms including Realms and Tribeca. It captures the initial creation of governance proposals, including proposal details, creator information, and proposal parameters, enabling comprehensive analysis of governance proposal lifecycle and creation patterns.

## Key Use Cases
- Track governance proposal creation patterns and frequency
- Analyze proposal creation by different governance platforms
- Study proposal lifecycle from creation to voting
- Monitor governance participation and proposal diversity
- Support DAO analytics and governance reporting

## Important Relationships
- Links to `gov.fact_proposal_votes` through `proposal` address for complete proposal lifecycle
- Connects to `gov.fact_gov_actions` for governance action analysis
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- Provides proposal context for governance analytics

## Commonly-used Fields
- `block_timestamp`: Timestamp when the proposal was created
- `tx_id`: Unique transaction identifier for the proposal creation
- `proposal`: Address representing the created proposal
- `creator`: Address of the account that created the proposal
- `governance_platform`: Platform where the proposal was created
- `program_name`: Name of the governance program
- `realms_id`: Address representing the voting group within Realms

{% enddocs %} 