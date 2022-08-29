{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH yawww_txs AS (

    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__events') }}
        e
    WHERE
        program_id = '5SKmrbAxnHV2sgqyDXkGrLrokZYtWWVEEk5Soed7VLVN'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    t.tx_id,
    t.block_timestamp,
    t.signers [0] :: STRING AS bidder,
    instructions [0] :accounts [2] :: STRING AS acct_2,
    MAX(i.value :parsed :info :lamports / pow(10, 9)) AS bid_amount
FROM
    {{ ref('silver__transactions') }}
    t
    INNER JOIN yawww_txs y
    ON t.tx_id = y.tx_id
    LEFT JOIN TABLE(FLATTEN(inner_instructions [0] :instructions)) i
    LEFT JOIN TABLE(FLATTEN(t.log_messages)) l
WHERE
    l.value :: STRING LIKE 'Program log: Instruction: Bid on listing'
    AND i.index = 3
    AND i.value :parsed :type :: STRING = 'transfer'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}
GROUP BY
    1,
    2,
    3,
    4
