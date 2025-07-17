{% docs bridge_ez_table_doc %}

## Description
This table provides a comprehensive view of cross-chain bridge activity on Solana, capturing token transfers via major protocols including Wormhole, DeBridge, and Mayan Finance. It standardizes inbound and outbound bridge transactions, includes protocol/platform identification, and enriches each event with USD pricing and token metadata where available. Each row represents a single bridge transaction, supporting analytics on cross-chain liquidity flows, protocol usage, and multi-chain DeFi activity.

## Key Use Cases
- Analyze cross-chain liquidity flows and bridge protocol adoption
- Track token movements between Solana and other blockchains
- Study user behavior and protocol usage patterns for bridging
- Monitor USD-denominated bridge flows and capital movement
- Support analytics on multi-chain DeFi ecosystem growth and integration

## Important Relationships
- Closely related to `defi.fact_bridge_activity` (raw bridge events), `defi.ez_liquidity_pool_actions` (for liquidity flows after bridging), and `defi.ez_dex_swaps` (for DEX activity of bridged tokens)
- Use `defi.fact_bridge_activity` for protocol-level bridge event details
- Use `defi.ez_liquidity_pool_actions` to analyze liquidity provision/removal after bridging
- Use `defi.ez_dex_swaps` to track trading activity of tokens post-bridge
- `core.fact_transactions` for transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series and bridge flow analysis
- `platform`: For protocol identification (Wormhole, DeBridge, Mayan, etc.)
- `direction`: For filtering inbound vs outbound transfers
- `source_chain`, `destination_chain`: For cross-chain analytics
- `source_address`, `destination_address`: For user and address-level analysis
- `amount`, `amount_usd`: For value and USD-denominated analytics
- `mint`, `symbol`, `token_is_verified`: For token and asset analytics
- `succeeded`: For transaction success analysis

{% enddocs %}

{% docs bridge_platform %}

The platform or protocol from which the bridge transaction or event originates.

{% enddocs %}

{% docs bridge_source_chain_sender %}

The address that initiated the bridge deposit or transfer. This address is the sender of the tokens/assets being bridged to the destination chain.

{% enddocs %}

{% docs bridge_destination_chain_receiver %}

The designated address set to receive the bridged tokens on the target chain after the completion of the bridge transaction.

{% enddocs %}

{% docs bridge_source_chain %}

The name of the blockchain network to which the assets are being bridged from.

{% enddocs %}

{% docs bridge_destination_chain %}

The name of the blockchain network to which the assets are being bridged to.

{% enddocs %}

{% docs bridge_direction %}

Indicates the direction in which the assets are being bridged, either inbound to Solana or outbound from Solana.

{% enddocs %}

{% docs bridge_token_address %}

The address associated with the token that is being bridged. It provides a unique identifier for the token within Solana.

{% enddocs %}

{% docs bridge_token_symbol %}

The symbol representing the token being bridged. This provides a shorthand representation of the token.

{% enddocs %}

{% docs bridge_amount_unadj %}

The raw, non-decimal adjusted amount of tokens involved in the bridge transaction. For Solana, these are decimal adjusted amounts.

{% enddocs %}

{% docs bridge_amount %}

The decimal adjusted amount of tokens involved in the bridge transaction, where available.

{% enddocs %}

{% docs bridge_amount_usd %}

The value of the bridged tokens in USD at the time of the bridge transaction, where available.

{% enddocs %}