{{ config(
    materialized = 'incremental',
    unique_key = ['sol_balances_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}
--add FR is false above

with balances as (SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    b.index,
    b.value:pubkey::string AS account,
    pre_balances[index]/ pow(10,9) AS pre_amount,
    post_balances[index]/ pow(10,9) AS post_amount,
    _inserted_timestamp
FROM
    {{ ref('silver__transactions') }}
    t,
    TABLE(FLATTEN(account_keys)) b
WHERE
    1 = 1
{% if is_incremental() %}
    {% if execute %}
    {{ get_batch_load_logic(this,15,'2024-06-09') }}
    {% endif %}
{% else %}
    and _inserted_timestamp::date between '2022-08-12' and '2022-09-01'
{% endif %}
)

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    account,
    'So11111111111111111111111111111111111111111' as mint,
    pre_amount,
    post_amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','account']) }} as sol_balances_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    balances
