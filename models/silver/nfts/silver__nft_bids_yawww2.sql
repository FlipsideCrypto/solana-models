{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION"
) }}

WITH yawww_txs AS (

    SELECT
        DISTINCT tx_id
    FROM
        {{ ref('silver__events2') }}
    WHERE
        block_timestamp :: date >= '2022-07-12'
        AND program_id = '5SKmrbAxnHV2sgqyDXkGrLrokZYtWWVEEk5Soed7VLVN'

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
    t.block_id,
    t.block_timestamp,
    t.tx_id,
    t.succeeded,
    t.signers [0] :: STRING AS bidder,
    instructions [0] :accounts [2] :: STRING AS acct_2,
    i.value :parsed :info :lamports / pow(
        10,
        9
    ) AS bid_amount,
    _inserted_timestamp
FROM
    {{ ref('silver__transactions2') }}
    t
    INNER JOIN yawww_txs y
    ON t.tx_id = y.tx_id
    LEFT JOIN TABLE(FLATTEN(inner_instructions [0] :instructions)) i
    LEFT JOIN TABLE(FLATTEN(t.log_messages)) l
WHERE
    l.value :: STRING LIKE 'Program log: Instruction: Bid on listing'
    AND i.index = 3
    AND i.value :parsed :type :: STRING = 'transfer'
    -- is this necessary? add this to filter out the transactions2 and make it run faster for a full load
    AND t.block_timestamp :: date >= '2022-07-12'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY t.tx_id
ORDER BY
    bid_amount DESC)) = 1
