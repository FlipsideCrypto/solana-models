{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE'],
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    b.index,
    b.value :accountIndex :: INTEGER AS account_index,
    t.account_keys [account_index] :pubkey :: STRING AS account,
    b.value :mint :: STRING AS mint,
    b.value :owner :: STRING AS owner,
    b.value :uiTokenAmount :amount :: INTEGER AS amount,
    b.value :uiTokenAmount :decimals AS DECIMAL,
    b.value :uiTokenAmount :uiAmount AS uiAmount,
    b.value :uiTokenAmount :uiAmountString AS uiAmountString,
    _inserted_timestamp
FROM
    {{ ref('silver__transactions2') }}
    t,
    TABLE(FLATTEN(post_token_balances)) b

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    t.block_timestamp :: DATE BETWEEN (
        SELECT
            LEAST(DATEADD(
                'day',
                1,
                COALESCE(MAX(block_timestamp) :: DATE, '2021-01-30')),'2022-09-19')
                FROM
                    {{ this }}
        )
        AND (
        SELECT
            LEAST(DATEADD(
            'day',
            30,
            COALESCE(MAX(block_timestamp) :: DATE, '2021-01-30')),'2022-09-19')
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
    t.block_timestamp :: DATE BETWEEN '2021-01-30'
    AND '2021-02-27' -- first month with token data in txs
{% endif %}
