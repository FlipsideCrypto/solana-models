{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','block_id','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION", 
    full_refresh = false
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
        t.data:transaction:message:addressTableLookups::array as address_table_lookups,
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
        and _partition_id = 15137)

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
    address_table_lookups,
    version,
    silver.udf_get_tx_size_test(account_keys,pre_balances,instructions,version,address_table_lookups) as tx_size_test,
    _partition_id,
    _inserted_timestamp
FROM
    pre_final b qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1

