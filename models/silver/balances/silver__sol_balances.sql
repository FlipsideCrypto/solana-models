{{ config(
    materialized = 'incremental',
    unique_key = ['sol_balances_id'],
    incremental_strategy = 'delete+insert',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
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

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 31319460)+1,175418104)
        FROM
            {{ this }}
        )
        AND (
        SELECT
            LEAST(COALESCE(MAX(block_id), 31319460)+8000000,175418104)
        FROM
            {{ this }}
        ) 
{% elif is_incremental() %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
{% else %}
WHERE
    -- block_id between 31319460 and 32319460
    block_id = 240134635
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
    '{{ invocation_id }}' AS invocation_id
FROM
    balances
