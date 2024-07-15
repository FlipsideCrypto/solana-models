{% docs sol_balance_table_doc %}

A fact table containing the beginning and ending balances of native SOL accounts in a transaction. This contains both successful and unsuccessful transactions as native SOL is still deducted in failures. The 'owner' is the same as the 'account_address' because native SOL is not held within a separate token account.

{% enddocs %}

{% docs token_balance_table_doc %}

A fact table containing the beginning and ending token balances of token accounts in a transaction. This only contain successful transactions as there are no token balance changes in failed transactions.

{% enddocs %}

{% docs balances_account %}

The account address which holds a specific token.

{% enddocs %}

{% docs balances_pre_amount %}

The initial decimal-adjusted amount in an account.

{% enddocs %}

{% docs balances_post_amount %}

The final decimal-adjusted amount in an account.

{% enddocs %}

{% docs balances_index %}

Location of the account entry within the balances array for a transaction.

{% enddocs %}

{% docs balances_account_index %}

Location corresponding to the index field in the account_keys array.

{% enddocs %}

{% docs token_balances_pre_owner %}

Owner of the token account at the start of the transaction.

{% enddocs %}

{% docs token_balances_post_owner %}

The final owner of the token account in the transaction.

{% enddocs %}

{% docs token_balances_block_owner %}

The final owner of the token account within the block.

{% enddocs %}

{% docs sol_balances_owner %}

The wallet holding the native SOL. This is represented as the same value as the 'account_address'.

{% enddocs %}


