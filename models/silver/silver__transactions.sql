{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['ingested_at::DATE']
) }}

WITH base AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        tx :transaction :message :recentBlockhash :: STRING AS recent_block_hash,
        tx :meta :fee :: NUMBER AS fee,
        CASE
            WHEN IS_NULL_VALUE(
                tx :meta :err
            ) THEN TRUE
            ELSE FALSE
        END AS succeeded,
        tx :transaction :message :accountKeys :: ARRAY AS account_keys,
        tx :meta :preBalances :: ARRAY AS pre_balances,
        tx :meta :postBalances :: ARRAY AS post_balances,
        tx :meta :preTokenBalances :: ARRAY AS pre_token_balances,
        tx :meta :postTokenBalances :: ARRAY AS post_token_balances,
        tx :transaction :message :instructions :: ARRAY AS instructions,
        tx :meta :innerInstructions :: ARRAY AS inner_instructions,
        tx :meta :logMessages :: ARRAY AS log_messages,
        ingested_at
    FROM
        {{ ref('bronze__transactions') }}
        t
    WHERE
        COALESCE(
            tx :transaction :message :instructions [0] :programId :: STRING,
            ''
        ) <> 'Vote111111111111111111111111111111111111111'

{% if is_incremental() %}
AND ingested_at :: DATE >= getdate() - INTERVAL '2 days'
{% endif %}
),
trans_tmp AS (
    SELECT
        block_timestamp,
        block_id,
        b.tx_id,
        recent_block_hash,
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
        ingested_at
    FROM
        base b 
        qualify(ROW_NUMBER() over(PARTITION BY b.block_id, b.tx_id
    ORDER BY
        b.ingested_at DESC)) = 1
),
signers_flattened AS (
    SELECT
        b.tx_id,
        A.index AS signer_index,
        A.value :pubkey :: STRING AS acct
    FROM
        trans_tmp b,
        TABLE(FLATTEN(b.account_keys)) A
    WHERE
        A.value :signer = TRUE
),
signers_arr AS (
    SELECT
        tx_id,
        ARRAY_AGG(acct) within GROUP (
            ORDER BY
                signer_index
        ) AS signers
    FROM
        signers_flattened
    GROUP BY
        1
)
SELECT
    block_timestamp,
    block_id,
    b.tx_id,
    recent_block_hash,
    s.signers AS signers,
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
    ingested_at
FROM
    trans_tmp b
    LEFT OUTER JOIN signers_arr s
    ON b.tx_id = s.tx_id
