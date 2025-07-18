{% docs fact_proposal_votes %}

## Description
This table captures all governance proposal votes across different platforms including Realms and Tribeca (Marinade). It tracks voting participation, vote choices, voting power, and governance decisions, providing comprehensive analytics on decentralized governance voting patterns and decision-making processes.

## Key Use Cases
- Analyze voting participation and patterns across governance platforms
- Track proposal outcomes and voting power distribution
- Study governance decision-making processes and voter behavior
- Monitor governance platform adoption and usage trends
- Support DAO analytics and governance reporting

## Important Relationships
- Links to `gov.fact_gov_actions` through governance context for comprehensive governance analysis
- Connects to `gov.fact_proposal_creation` for proposal lifecycle tracking
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- Provides voting context for protocol-specific governance analytics

## Commonly-used Fields
- `block_timestamp`: Timestamp when the vote was cast
- `tx_id`: Unique transaction identifier for the vote
- `voter`: Address of the account that cast the vote
- `voter_account`: Account with locked tokens linked to the NFT (determines voting power)
- `voter_nft`: NFT mint used in this vote on Marinade
- `proposal`: Address representing the proposal being voted on
- `governance_platform`: Platform used for governance (e.g., 'realms', 'tribeca')
- `program_name`: Name of the governance program
- `vote_choice`: The voting option selected by the user on a Realms proposal
- `vote_rank`: The order which a user ranks their choices on a ranked vote
- `vote_weight`: Percentage of voting power put towards a voting option
- `realms_id`: Address representing the voting group within Realms

{% enddocs %} 