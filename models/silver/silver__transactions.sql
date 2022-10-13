{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','block_id','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}

WITH pre_final AS (

    SELECT
        COALESCE(TO_TIMESTAMP_NTZ(t.value :block_time), b.block_timestamp) AS block_timestamp,
        t.block_id,
        t.tx_id,
        t.data :transaction :message :recentBlockhash :: STRING AS recent_block_hash,
        t.data :meta :fee :: NUMBER AS fee,
        CASE
            WHEN IS_NULL_VALUE(
                t.data :meta :err
            ) THEN TRUE
            ELSE FALSE
        END AS succeeded,
        t.data :transaction :message :accountKeys :: ARRAY AS account_keys,
        t.data :meta :preBalances :: ARRAY AS pre_balances,
        t.data :meta :postBalances :: ARRAY AS post_balances,
        t.data :meta :preTokenBalances :: ARRAY AS pre_token_balances,
        t.data :meta :postTokenBalances :: ARRAY AS post_token_balances,
        t.data :transaction :message :instructions :: ARRAY AS instructions,
        t.data :meta :innerInstructions :: ARRAY AS inner_instructions,
        t.data :meta :logMessages :: ARRAY AS log_messages,
        t.data :version :: STRING as version,
        t._partition_id,
        t._inserted_timestamp
    FROM
        {{ ref('bronze__transactions2') }}
        t
        LEFT OUTER JOIN {{ ref('silver__blocks') }}
        b
        ON b.block_id = t.block_id
    WHERE
        tx_id IS NOT NULL
        AND COALESCE(
            t.data :transaction :message :instructions [0] :programId :: STRING,
            ''
        ) <> 'Vote111111111111111111111111111111111111111'

{% if is_incremental() %}
AND _partition_id >= (
    SELECT
        MAX(_partition_id) -1
    FROM
        {{ this }}
)
AND _partition_id <= (
    SELECT
        MAX(_partition_id) + 10
    FROM
        {{ this }}
)
AND t._inserted_timestamp > (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
    AND _partition_id IN (
        1,
        2
    )
{% endif %}
)

{% if is_incremental() %},
prev_null_block_timestamp_txs AS (
    SELECT
        b.block_timestamp,
        t.block_id,
        t.tx_id,
        t.recent_block_hash,
        t.signers,
        t.fee,
        t.succeeded,
        t.account_keys,
        t.pre_balances,
        t.post_balances,
        t.pre_token_balances,
        t.post_token_balances,
        t.instructions,
        t.inner_instructions,
        t.log_messages,
        t.version,
        t._partition_id,
        GREATEST(
            t._inserted_timestamp,
            b._inserted_timestamp
        ) AS _inserted_timestamp
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__blocks') }}
        b
        ON b.block_id = t.block_id
    WHERE
        t.block_timestamp :: DATE IS NULL
        AND t.block_id > 39824213
)
{% endif %}
SELECT
    block_timestamp,
    block_id,
    tx_id,
    recent_block_hash,
    silver.udf_ordered_signers(account_keys) AS signers,
    fee,
    succeeded,
    account_keys,
    pre_balances,
    post_balances,
    pre_token_balances,
    post_token_balances,
    instructions,
    inner_instructions,
    log_messages,
    version,
    _partition_id,
    _inserted_timestamp
FROM
    pre_final b qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1

{% if is_incremental() %}
UNION
SELECT
    *
FROM
    prev_null_block_timestamp_txs
{% endif %}
