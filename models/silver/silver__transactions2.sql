{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','block_id','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}

WITH pre_final AS (
    SELECT
        b.block_timestamp,
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
        t._partition_id,
        t._inserted_timestamp
    FROM
        {{ ref('bronze__transactions2') }} t
    LEFT OUTER JOIN 
        {{ ref('silver__blocks2') }} b on b.block_id = t.block_id
    WHERE
        tx_id is not null
    AND 
        COALESCE(
            t.data :transaction :message :instructions [0] :programId :: STRING,
            ''
        ) <> 'Vote111111111111111111111111111111111111111'
    {% if is_incremental() %}
    AND 
        _partition_id >= (
            select max(_partition_id)-1
            from {{this}}
        )
    AND
        _partition_id <= (
            select max(_partition_id)+10
            from {{this}}
        )
    AND 
        t._inserted_timestamp > (
            select max(_inserted_timestamp)
            from {{this}}
        )
    {% else %}
    AND 
        _partition_id in (1,2)
    {% endif %}
)
{% if is_incremental() %}
, prev_null_block_timestamp_txs as (
    select 
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
        t._partition_id,
        greatest(t._inserted_timestamp,b._inserted_timestamp) as _inserted_timestamp
    from {{ this }} t
    inner join {{ ref('silver__blocks2') }} b on b.block_id = t.block_id
    where t.block_timestamp::date is null
    and t.block_id > 39824213;
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
    _partition_id,
    _inserted_timestamp
FROM
    pre_final b 
qualify(ROW_NUMBER() over(PARTITION BY block_id, tx_id
ORDER BY
    _inserted_timestamp DESC)) = 1
{% if is_incremental() %}
union
select *
from prev_null_block_timestamp_txs
{% endif %}