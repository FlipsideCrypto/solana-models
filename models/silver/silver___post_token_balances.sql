{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id, index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE'],
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
    ingested_at,
    _inserted_timestamp
FROM
    {{ ref('silver__transactions') }}
    t,
    TABLE(FLATTEN(post_token_balances)) b
{% if is_incremental() %}
WHERE
    ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}
