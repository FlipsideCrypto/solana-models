{% docs lending_platform %}
The name of the lending platform or protocol where the transaction occurred. This identifies the specific DeFi lending service provider.

**Data type:** STRING
**Business context:** Used to categorize lending activity by platform, analyze platform-specific metrics, and compare lending volumes across different protocols.
**Analytics use cases:** Platform performance analysis, market share tracking, and cross-platform lending behavior studies.
**Example:** 'kamino', 'marginfi v2'
{% enddocs %}

{% docs lending_protocol %}
The core protocol name that powers the lending platform. This provides a standardized identifier for the underlying lending technology.

**Data type:** STRING
**Business context:** Used to group related platforms by their underlying protocol technology, enabling analysis of protocol adoption and usage.
**Analytics use cases:** Protocol ecosystem analysis, technology adoption tracking, and protocol performance comparisons.
**Example:** 'kamino', 'marginfi'
{% enddocs %}

{% docs version %}
The version identifier of the lending protocol being used. This helps track different iterations and upgrades of lending protocols.

**Data type:** STRING
**Business context:** Used to analyze adoption of protocol upgrades, compare performance across versions, and track protocol evolution.
**Analytics use cases:** Version adoption analysis, upgrade impact assessment, and historical protocol development tracking.
**Example:** 'v1', 'v2'
{% enddocs %}

{% docs amount_raw %}
Unadjusted amount of tokens as it appears on-chain before decimal precision adjustments are applied. This preserves the exact on-chain representation of the token amount for precise calculations and verification.

**Data type:** NUMBER
**Business context:** Used for precise calculations, audit trails, and verification against on-chain data. Essential for maintaining data integrity and performing exact mathematical operations without rounding errors.
**Analytics use cases:** Precision calculations, data validation, audit verification, and exact token accounting for high-value transactions.
**Example:** For 1.5 USDC (6 decimals), amount_raw would be 1500000; for 2.0 SOL (9 decimals), amount_raw would be 2000000000
{% enddocs %}

{% docs depositor %}
The wallet address of the user who is depositing assets into the lending protocol. This is the lender who supplies liquidity to earn interest and potentially use their deposits as collateral for borrowing.

**Data type:** STRING (base58 Solana address)
**Business context:** Used to track deposit behavior, analyze liquidity provision patterns, and identify active lenders in the lending ecosystem.
**Analytics use cases:** Depositor behavior analysis, liquidity tracking, yield farming analysis, and lender user segmentation.
**Example:** '4Nd1mYw4r...'
{% enddocs %}

{% docs lending_borrower %}
The wallet address of the user who is borrowing assets from the lending protocol. This is the recipient of the borrowed funds who becomes responsible for repayment.

**Data type:** STRING (base58 Solana address)
**Business context:** Used to track borrowing behavior, analyze user borrowing patterns, and identify active borrowers in the lending ecosystem.
**Analytics use cases:** Borrower behavior analysis, loan tracking, credit risk assessment, and user segmentation.
**Example:** '4Nd1mYw4r...'
{% enddocs %}

{% docs lending_withdrawer %}
The wallet address of the user who is withdrawing their deposited assets from the lending protocol. This represents the lender retrieving their funds and accrued interest.

**Data type:** STRING (base58 Solana address)
**Business context:** Used to track withdrawal patterns, analyze liquidity provision behavior, and monitor capital flows out of lending protocols.
**Analytics use cases:** Withdrawal behavior analysis, liquidity tracking, and lender activity monitoring.
**Example:** '4Nd1mYw4r...'
{% enddocs %}

{% docs lending_payer %}
The wallet address of the user who is making a repayment on a loan. This may be the original borrower or a third party paying on behalf of the borrower.

**Data type:** STRING (base58 Solana address)
**Business context:** Used to track repayment activity, analyze loan performance, and identify who is servicing debt obligations.
**Analytics use cases:** Repayment behavior analysis, loan performance tracking, and debt service monitoring.
**Example:** '4Nd1mYw4r...'
{% enddocs %}

{% docs lending_liquidator %}
The wallet address of the user or bot that initiated a liquidation event. This is the party that repays the debt and receives the collateral at a discount.

**Data type:** STRING (base58 Solana address)
**Business context:** Used to track liquidation activity, analyze liquidator behavior, and monitor risk management in lending protocols.
**Analytics use cases:** Liquidation analysis, risk assessment, liquidator profitability studies, and protocol health monitoring.
**Example:** '4Nd1mYw4r...'
{% enddocs %}

{% docs protocol_market %}
The protocol-specific token or market identifier that represents the lending pool or reserve. This is typically a wrapped version of the underlying asset used by the protocol for accounting.

**Data type:** STRING (base58 Solana address)
**Business context:** Used to identify specific lending markets within protocols, track market-specific metrics, and analyze asset utilization rates.
**Analytics use cases:** Market performance analysis, asset utilization tracking, and protocol-specific lending pool analytics.
**Example:** 'cETH', 'aUSDC', or protocol-specific market tokens
{% enddocs %}

{% docs lending_collateral_token %}
The token address of the asset that was used as collateral and is being seized during a liquidation event. This represents the asset the borrower loses to cover their debt.

**Data type:** STRING (base58 Solana address)
**Business context:** Used to track which assets are commonly used as collateral and which are frequently liquidated, helping assess collateral risk.
**Analytics use cases:** Collateral analysis, liquidation risk assessment, and asset safety evaluation in lending protocols.
**Example:** 'So11111111111111111111111111111111111111112' (SOL)
{% enddocs %}

{% docs lending_debt_token %}
The token address of the asset that was borrowed and is being repaid during a liquidation event. This represents the debt that the liquidator is covering.

**Data type:** STRING (base58 Solana address)
**Business context:** Used to track which assets are commonly borrowed and defaulted on, helping assess lending risk by asset type.
**Analytics use cases:** Debt analysis, default risk assessment, and borrowing pattern analysis in lending protocols.
**Example:** 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' (USDC)
{% enddocs %}

{% docs collateral_token_symbol %}
The human-readable symbol of the collateral token being seized during liquidation. This provides an easy-to-understand identifier for the collateral asset.

**Data type:** STRING
**Business context:** Used for reporting and analytics to make collateral assets easily identifiable without needing to decode token addresses.
**Analytics use cases:** Collateral reporting, risk analysis dashboards, and user-friendly liquidation analytics.
**Example:** 'SOL', 'ETH', 'BTC'
{% enddocs %}

{% docs debt_token_symbol %}
The human-readable symbol of the debt token being repaid during liquidation. This provides an easy-to-understand identifier for the debt asset.

**Data type:** STRING
**Business context:** Used for reporting and analytics to make debt assets easily identifiable without needing to decode token addresses.
**Analytics use cases:** Debt reporting, risk analysis dashboards, and user-friendly liquidation analytics.
**Example:** 'USDC', 'USDT', 'SOL'
{% enddocs %}

{% docs ez_lending_deposits %}

## Description
This table captures deposit events across Solana DeFi lending protocols including Kamino and MarginFi. Each row represents a single deposit transaction where users supply assets to lending pools to earn passive income and provide collateral for potential borrowing. The data includes enriched transaction details with USD pricing, token metadata, and protocol identification, supporting comprehensive analytics on lending market supply-side activity and user behavior.

## Key Use Cases
- Track deposit flows and liquidity provision patterns across lending protocols
- Analyze user deposit behavior and capital allocation strategies
- Monitor total value locked (TVL) growth and composition in lending markets
- Study protocol adoption and user preferences in the lending ecosystem
- Calculate interest accrual and yield analysis for deposited assets
- Support risk assessment and collateralization analysis

## Important Relationships
- Related to `defi.ez_lending_borrows` for analyzing borrowing against deposits
- Related to `defi.ez_lending_withdraws` for tracking deposit lifecycle completion
- Connects to `core.ez_transfers` for underlying asset movement verification
- Links to `price.ez_prices_hourly` for USD valuation and pricing analysis
- Joins with `core.fact_transactions` for complete transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series analysis and deposit timing patterns
- `platform`, `protocol`: For protocol-specific analysis and market share tracking
- `depositor`: For user behavior analysis and whale deposit tracking
- `token_address`, `token_symbol`: For asset-specific deposit analysis
- `amount`, `amount_usd`: For volume analysis and value-based metrics
- `protocol_market`: For market-specific utilization and performance analysis

{% enddocs %}

{% docs ez_lending_borrows %}

## Description
This table captures borrow events across Solana DeFi lending protocols including Kamino and MarginFi. Each row represents a single borrowing transaction where users take loans against their deposited collateral. The data includes enriched transaction details with USD pricing, token metadata, and protocol identification, supporting comprehensive analytics on lending market demand-side activity and borrowing behavior patterns.

## Key Use Cases
- Track borrowing activity and capital utilization across lending protocols
- Analyze user borrowing patterns and leverage strategies
- Monitor borrowed asset composition and demand trends
- Study protocol usage and borrowing preference analysis
- Calculate borrowing costs and interest rate analysis
- Support credit risk assessment and loan performance tracking

## Important Relationships
- Related to `defi.ez_lending_deposits` for analyzing collateral backing borrowed amounts
- Related to `defi.ez_lending_repayments` for tracking loan lifecycle and repayment behavior
- Related to `defi.ez_lending_liquidations` for analyzing defaults and risk events
- Connects to `core.ez_transfers` for underlying asset movement verification
- Links to `price.ez_prices_hourly` for USD valuation and loan-to-value calculations

## Commonly-used Fields
- `block_timestamp`: For time-series analysis and borrowing timing patterns
- `platform`, `protocol`: For protocol-specific analysis and borrowing market share
- `borrower`: For user behavior analysis and borrowing pattern identification
- `token_address`, `token_symbol`: For asset-specific borrowing demand analysis
- `amount`, `amount_usd`: For borrowing volume analysis and market size metrics
- `protocol_market`: For market-specific borrowing rates and utilization tracking

{% enddocs %}

{% docs ez_lending_withdraws %}

## Description
This table captures withdrawal events across Solana DeFi lending protocols including Kamino and MarginFi. Each row represents a single withdrawal transaction where users retrieve their deposited assets along with accrued interest from lending pools. The data includes enriched transaction details with USD pricing, token metadata, and protocol identification, supporting comprehensive analytics on lending market liquidity flows and user exit behavior.

## Key Use Cases
- Track withdrawal patterns and liquidity outflows from lending protocols
- Analyze user exit behavior and fund reallocation strategies
- Monitor protocol liquidity health and withdrawal capacity
- Study seasonal patterns and market-driven withdrawal trends
- Calculate realized yields and interest earnings on withdrawals
- Support liquidity risk assessment and protocol stability analysis

## Important Relationships
- Related to `defi.ez_lending_deposits` for analyzing complete deposit-withdraw lifecycle
- Connects to `core.ez_transfers` for underlying asset movement verification
- Links to `price.ez_prices_hourly` for USD valuation and yield calculations
- Joins with `core.fact_transactions` for complete transaction context
- Related to market stress events that may trigger withdrawal spikes

## Commonly-used Fields
- `block_timestamp`: For time-series analysis and withdrawal timing patterns
- `platform`, `protocol`: For protocol-specific liquidity and withdrawal analysis
- `depositor`: For user behavior analysis and withdrawal pattern identification
- `token_address`, `token_symbol`: For asset-specific withdrawal demand analysis
- `amount`, `amount_usd`: For withdrawal volume analysis and liquidity flow metrics
- `protocol_market`: For market-specific withdrawal rates and capacity tracking

{% enddocs %}

{% docs ez_lending_repayments %}

## Description
This table captures loan repayment events across Solana DeFi lending protocols including Kamino and MarginFi. Each row represents a single repayment transaction where borrowers or third parties pay back borrowed assets plus accrued interest. The data includes enriched transaction details with USD pricing, token metadata, and protocol identification, supporting comprehensive analytics on loan performance, repayment behavior, and credit risk assessment.

## Key Use Cases
- Track loan repayment patterns and borrower payment behavior
- Analyze repayment timing and loan performance metrics
- Monitor protocol health through repayment rates and defaults
- Study interest payment flows and protocol revenue generation
- Calculate loan duration and repayment cycle analysis
- Support credit risk modeling and borrower risk assessment

## Important Relationships
- Related to `defi.ez_lending_borrows` for analyzing complete loan lifecycle from origination to repayment
- Related to `defi.ez_lending_liquidations` for understanding default resolution alternatives
- Connects to `core.ez_transfers` for underlying asset movement verification
- Links to `price.ez_prices_hourly` for USD valuation and interest calculations
- Joins with `core.fact_transactions` for complete transaction context

## Commonly-used Fields
- `block_timestamp`: For time-series analysis and repayment timing patterns
- `platform`, `protocol`: For protocol-specific repayment and performance analysis
- `payer`, `borrower`: For user behavior analysis and repayment responsibility tracking
- `token_address`, `token_symbol`: For asset-specific repayment analysis
- `amount`, `amount_usd`: For repayment volume analysis and interest calculation
- `protocol_market`: For market-specific repayment rates and loan performance

{% enddocs %}

{% docs ez_lending_liquidations %}

## Description
This table captures liquidation events across Solana DeFi lending protocols including Kamino and MarginFi. Each row represents a single liquidation transaction where underwater loans are forcibly closed by liquidators who repay the debt and receive collateral at a discount. The data includes enriched transaction details with USD pricing for both debt and collateral tokens, supporting comprehensive analytics on protocol risk management, market stress events, and liquidation ecosystem dynamics.

## Key Use Cases
- Track liquidation activity and protocol risk management effectiveness
- Analyze market stress events and borrower default patterns
- Monitor liquidator behavior and profitability in risk management
- Study collateral safety and asset-specific liquidation risks
- Calculate liquidation penalties and protocol protection mechanisms
- Support risk modeling and collateral requirement optimization

## Important Relationships
- Related to `defi.ez_lending_borrows` for analyzing loans that resulted in liquidation
- Connected to market volatility events that trigger liquidation cascades
- Links to `price.ez_prices_hourly` for both debt and collateral token valuations
- Joins with `core.fact_transactions` for complete transaction context
- Related to protocol-specific liquidation parameters and thresholds

## Commonly-used Fields
- `block_timestamp`: For time-series analysis and liquidation timing during market events
- `platform`, `protocol`: For protocol-specific liquidation mechanism analysis
- `liquidator`, `borrower`: For liquidation ecosystem participant behavior analysis
- `debt_token`, `collateral_token`: For asset-specific liquidation risk assessment
- `debt_token_symbol`, `collateral_token_symbol`: For human-readable asset identification
- `amount`, `amount_usd`: For liquidation volume analysis and market impact assessment
- `protocol_market`: For market-specific liquidation thresholds and risk parameters

{% enddocs %}
