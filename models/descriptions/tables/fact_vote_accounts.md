{% docs fact_vote_accounts %}

## Description
This table tracks vote accounts on the Solana blockchain, capturing vote account creation, updates, and operational status. Vote accounts are essential components of Solana's consensus mechanism, representing the voting power of validators and enabling stake-weighted voting on network decisions.

## Key Use Cases
- Track vote account creation and lifecycle management
- Analyze vote account performance and voting participation
- Monitor vote account balance changes and stake distribution
- Study validator voting patterns and network consensus
- Support staking analytics and validator performance analysis

## Important Relationships
- Links to `gov.fact_validators` through `vote_pubkey` for validator-vote account mapping
- Connects to `gov.fact_stake_accounts` for stake distribution analysis
- References `core.fact_blocks` and `core.fact_transactions` for blockchain context
- Provides vote account context for `gov.fact_rewards_voting`

## Commonly-used Fields
- `block_timestamp`: Timestamp when the vote account event occurred
- `tx_id`: Unique transaction identifier for the vote account event
- `vote_pubkey`: Address of the vote account
- `node_pubkey`: Address of the validator node associated with the vote account
- `authorized_voter`: Address authorized to vote on behalf of the vote account
- `authorized_withdrawer`: Address authorized to withdraw from the vote account
- `commission`: Commission rate for the vote account
- `vote_balance`: Current voting balance of the account
- `vote_credits`: Vote credits earned by the account

{% enddocs %} 