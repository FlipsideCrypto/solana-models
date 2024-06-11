{{ config(
    materialized = 'incremental',
    unique_key = ['fact_token_balances_id'],
    incremental_strategy = 'merge',
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::DATE"],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id, account_address);",
    tags = ['scheduled_non_core']
) }}
-- what should i cluster by?

SELECT
    a.block_timestamp,
    a.block_id,
    a.tx_id,
    a.succeeded,
    a.account as account_address,
    a.mint,
    b.owner as owner,
    a.pre_token_amount as pre_balance,
    a.post_token_amount as balance,
    a.token_balances_id as fact_token_balances_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp
FROM
    {{ ref('silver__token_balances') }} a
left join {{ ref('silver__token_account_owners') }} b
on a.account = b.account_address
    and a.block_id >= b.start_block_id
    and (a.block_id < b.end_block_id or b.end_block_id is null)

{% if is_incremental() %}
AND a.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
