{{ config(
    materialized = 'incremental',
    unique_key = ['fact_token_balances_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, account_address)'),
    tags = ['scheduled_non_core']
) }}

SELECT
    A.block_timestamp,
    A.block_id,
    A.tx_id,
    A.succeeded,
    A.account AS account_address,
    A.mint,
    b.owner AS owner,
    A.pre_token_amount AS pre_balance,
    A.post_token_amount AS balance,
    A.token_balances_id AS fact_token_balances_id,
    A.inserted_timestamp,
    A.modified_timestamp
FROM
    {{ ref('silver__token_balances') }} A
    LEFT JOIN {{ ref('silver__token_account_owners') }}
    b
    ON A.account = b.account_address
    AND A.block_id >= b.start_block_id
    AND (
        A.block_id < b.end_block_id
        OR b.end_block_id IS NULL
    )

{% if is_incremental() %}
WHERE A.modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
