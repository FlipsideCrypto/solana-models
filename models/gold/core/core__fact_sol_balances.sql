{{ config(
    materialized = 'incremental',
    unique_key = ['fact_sol_balances_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id, account_address)'),
    tags = ['scheduled_non_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    tx_index,
    succeeded,
    account AS account_address,
    mint,
    account AS owner,
    pre_amount AS pre_balance,
    post_amount AS balance,
    sol_balances_id AS fact_sol_balances_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__sol_balances') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
