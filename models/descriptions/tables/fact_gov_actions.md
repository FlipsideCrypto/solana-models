{% docs fact_gov_actions %}

## Description
This table tracks all governance actions across Saber and Marinade governance platforms. It captures various governance activities such as voting, proposal creation, and administrative actions, providing a comprehensive view of decentralized governance participation and decision-making processes on Solana.

## Key Use Cases
- Analyze governance participation patterns across different protocols
- Track voting behavior and decision-making processes
- Monitor governance action types and their frequency
- Study protocol governance evolution and participation trends
- Support governance analytics and reporting for DAOs and protocols

## Important Relationships
- Links to `gov.fact_proposal_votes` through governance context for comprehensive voting analysis
- Connects to `gov.fact_proposal_creation` for proposal lifecycle tracking
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- Provides governance context for protocol-specific analytics

## Commonly-used Fields
- `block_timestamp`: Timestamp when the governance action was processed
- `tx_id`: Unique transaction identifier for the governance action
- `signer`: Address of the account that performed the governance action
- `program_name`: Name of the governance program (e.g., 'saber', 'marinade')
- `action`: Type of governance action performed
- `amount`: Amount associated with the governance action
- `locker_account`: Account with locked tokens for governance participation
- `locker_nft`: NFT used for governance participation (Marinade)
- `mint`: Token mint address associated with the governance action

{% enddocs %} 