{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','block_id','_inserted_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}
/* regular load
add an extra incremental CTE for all txs w/ null blokc timestamps to try to get block timestamp if available. use greateer of the blk_inserted_ts or tx_inserted_ts as the ts"
union that cte at the end if incremental */

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
        t._inserted_timestamp > (
            select max(_inserted_timestamp)
            from {{this}}
        )
    {% else %}
    AND 
        _partition_id = 1
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
    where block_timestamp::date is null
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

/*WITH base AS (

    SELECT
        * (
            SELECT
                block_id
            FROM
                {{ ref('silver__blocks2') }}
            EXCEPT
            SELECT
                DISTINCT block_id
            FROM
                {{ this }}
        )
    LIMIT
        10000
), partitions_to_search AS (
    SELECT
        _partition_id,
        t.block_id
    FROM
        {{ ref('streamline__complete_block_txs') }}
        t
        INNER JOIN base
        ON b.block_id = t.block_id
),
pre_final AS (
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
        _partition_id,
        _inserted_timestamp
    FROM
        {{ ref('bronze__transactions2') }}
        t
        INNER JOIN partitions_to_search p
        ON p._partition_id = t._partition_id
        AND p.block_id = t.block_id
        JOIN {{ ref('silver__blocks2') }}
        b
        ON b.block_id = t.block_id
)
SELECT
    block_timestamp,
    block_id,
    b.tx_id,
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
    ingested_at,
    _inserted_timestamp
FROM
    pre_final b
qualify(ROW_NUMBER() over(PARTITION BY b.block_id, b.tx_id
ORDER BY
    b._inserted_timestamp DESC)) = 1

    
 WITH max_partition AS (
        SELECT
            COALESCE(MAX(_partition_id), 1) AS _partition_id
        FROM
            {{ this }}
    ),
    pre_final as (
        SELECT
            b.block_timestamp,
            t.block_id,
            tx_id,
            DATA :transaction :message :recentBlockhash :: STRING AS recent_block_hash,
            DATA :meta :fee :: NUMBER AS fee,
            CASE
                WHEN IS_NULL_VALUE(
                    DATA :meta :err
                ) THEN TRUE
                ELSE FALSE
            END AS succeeded,
            DATA :transaction :message :accountKeys :: ARRAY AS account_keys,
            DATA :meta :preBalances :: ARRAY AS pre_balances,
            DATA :meta :postBalances :: ARRAY AS post_balances,
            DATA :meta :preTokenBalances :: ARRAY AS pre_token_balances,
            DATA :meta :postTokenBalances :: ARRAY AS post_token_balances,
            DATA :transaction :message :instructions :: ARRAY AS instructions,
            DATA :meta :innerInstructions :: ARRAY AS inner_instructions,
            DATA :meta :logMessages :: ARRAY AS log_messages,
            _partition_id,
            _inserted_timestamp
        FROM
            {{ ref('bronze__transactions2') }}
            t
            INNER JOIN {{ ref('silver__blocks2') }}
            b
            ON t.block_id = b.block_id
        WHERE
            _partition_id >= 2200 ---AND t._partition_id <= 1499
            AND (
                error IS NULL
                OR error :code <> '-32009'
            ) -- block is empty
            AND COALESCE(
                DATA :transaction :message :instructions [0] :programId :: STRING,
                ''
            ) <> 'Vote111111111111111111111111111111111111111'
        {% if is_incremental() %}
        AND _partition_id >= (
            SELECT
                MAX(_partition_id)
            FROM
                max_partition
        )
        {% endif %}*/
