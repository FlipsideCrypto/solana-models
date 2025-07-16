{% docs sol_balance_table_doc %}

Contains one row per account and transaction where a native SOL (Solana) balance changes on the Solana blockchain. Tracks pre- and post-transaction balances for each account, including block, transaction, and mint information. Balances are adjusted for SOL's 9 decimal places. Enables analysis of SOL balance changes, account activity, and ownership attribution at the transaction level. Includes both successful and unsuccessful transactions, as native SOL is deducted even in failures. The 'owner' is the same as the 'account_address' because native SOL is not held within a separate token account. Data is updated incrementally as new blocks are processed, and each row represents a unique account-transaction balance change event.

{% enddocs %}

{% docs token_balance_table_doc %}

Contains one row per account and transaction where an SPL token balance changes on the Solana blockchain. Tracks pre- and post-transaction balances for each token account, including block, transaction, mint, and owner information. Balances are decimal adjusted according to the token's mint. Enables analysis of token balance changes, account activity, and ownership attribution at the transaction level. Only includes successful transactions, as there are no token balance changes in failed transactions. Data is updated incrementally as new blocks are processed, and each row represents a unique token account-transaction balance change event.

{% enddocs %}

{% docs balances_account %}

The base58-encoded address of the account holding the asset. For native SOL, this is the wallet address. For SPL tokens, this is the token account address. Used to attribute balances and transfers to specific accounts.

**Example:**
- Native SOL: `7GgkQ2...` (wallet address)
- SPL Token: `9xQeWv...` (token account address)

{% enddocs %}

{% docs balances_pre_amount %}

The account's balance before the transaction, decimal-adjusted. For SOL, this is in units of SOL (not lamports); for tokens, this is in the token's native units (adjusted for mint decimals).

**Data type:** Numeric (decimal)
**Example:**
- SOL: `1.23456789` (represents 1.23456789 SOL)
- USDC: `100.00` (represents 100 USDC tokens)

{% enddocs %}

{% docs balances_post_amount %}

The account's balance after the transaction, decimal-adjusted. For SOL, this is in units of SOL (not lamports); for tokens, this is in the token's native units (adjusted for mint decimals).

**Data type:** Numeric (decimal)
**Example:**
- SOL: `0.23456789` (represents 0.23456789 SOL after a transfer)
- USDC: `50.00` (represents 50 USDC tokens after a transfer)

{% enddocs %}

{% docs balances_index %}

The position of the account entry within the balances array for a transaction. Used to correlate balances with other transaction-level arrays (e.g., account_keys).

**Data type:** Integer
**Example:**
- `0` (first account in the array)
- `2` (third account in the array)

{% enddocs %}

{% docs balances_account_index %}

The index of the account in the account_keys array for the transaction. Useful for mapping balances to specific accounts referenced in the transaction.

**Data type:** Integer
**Example:**
- `1` (second account in account_keys)

{% enddocs %}

{% docs token_balances_pre_owner %}

The owner of the token account at the start of the transaction. This is a base58-encoded address. Ownership may change during a transaction (e.g., via token transfers or account reassignment).

**Data type:** String (base58 address)
**Example:**
- `7GgkQ2...`

{% enddocs %}

{% docs token_balances_post_owner %}

The owner of the token account at the end of the transaction. This is a base58-encoded address. If ownership changes during the transaction, this field reflects the new owner.

**Data type:** String (base58 address)
**Example:**
- `9xQeWv...`

{% enddocs %}

{% docs token_balances_block_owner %}

The owner of the token account at the end of the block. Used for block-level attribution of balances and transfers. This is a base58-encoded address.

**Data type:** String (base58 address)
**Example:**
- `7GgkQ2...`

{% enddocs %}

{% docs sol_balances_owner %}

The wallet address holding the native SOL. For native SOL, the owner is always the same as the account address, since SOL is not held in a separate token account.

**Data type:** String (base58 address)
**Example:**
- `7GgkQ2...`

{% enddocs %}


