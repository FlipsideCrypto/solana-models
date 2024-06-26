{{ config(
    materialized = 'incremental',
    unique_key = ['sol_balances_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    full_refresh = false,
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}

WITH balances AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        b.index,
        b.value :pubkey :: STRING AS account,
        pre_balances [index] / pow(
            10,
            9
        ) AS pre_amount,
        post_balances [index] / pow(
            10,
            9
        ) AS post_amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__transactions') }} t,
        TABLE(FLATTEN(account_keys)) b
    WHERE
        1 = 1

{% if is_incremental() %}
{% if execute %}
    {{ get_batch_load_logic( this, 15, '2024-06-09') }}
{% endif %}
{% else %}
    AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-01'
{% endif %}
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    INDEX,
    account,
    'So11111111111111111111111111111111111111111' AS mint,
    pre_amount,
    post_amount,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','account']) }} AS sol_balances_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    balances
WHERE 
    pre_amount <> post_amount
