{% docs fact_token_daily_balances %}

## Description
This table provides daily snapshots of token balances for each account and mint combination on the Solana blockchain. It creates a complete time series by forward-filling the most recent balance when there's no activity on a given day, ensuring every account-mint combination has a balance record for each day since their first balance change. When multiple balance updates occur within a day, only the last balance is retained, providing a consistent end-of-day view. 

**Important:** If an account is left with a 0 balance at the end of the day, it is not included in the table. This means account-mint combinations will "disappear" from the daily snapshots when their balance reaches zero and "reappear" when they receive tokens again.

## Key Use Cases
- Daily balance tracking and portfolio analysis over time
- Time-series analysis of token holdings and distribution patterns
- Historical balance lookups for any account-mint combination on any date
- Whale tracking and large holder analysis with daily granularity
- DeFi protocol analytics requiring daily balance snapshots
- Token distribution studies and holder concentration analysis

## Important Relationships
- Sources data from `core.fact_token_balances` for balance change events
- Links to `core.fact_token_account_owners` through `post_owner` for ownership attribution
- Connects to `price.ez_asset_metadata` via `mint` for token metadata and pricing
- Joins with `core.ez_transfers` for transfer context and flow analysis

## Commonly-used Fields
- `balance_date`: The date for the balance snapshot (primary time dimension)
- `account`: Token account address holding the balance
- `mint`: Token mint address identifying the specific token
- `amount`: The token balance amount (decimal adjusted) at end of day
- `owner`: The owner of the token account (for attribution)
- `last_balance_change`: The last date when this account's balance actually changed (only tracks dates when account had a positive balance)
- `balance_changed_on_date`: Boolean indicating if the balance changed on this specific date

{% enddocs %}
