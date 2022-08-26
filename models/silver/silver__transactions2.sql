{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}

{% if is_incremental() %}
WITH max_partition AS (

    SELECT
        MAX(
            _partition_id
        ) _partition_id
    FROM
        {{ this }}
)
{% endif %}

SELECT 
    b.block_timestamp, 
    t.block_id, 
    tx_id, 
    TRIM(data :transaction :message :recentBlockhash, '"') AS recent_block_hash,
    data :meta :fee :: NUMBER AS fee, 
    CASE 
        WHEN IS_NULL_VALUE(
            data :meta :err
        ) THEN TRUE
        ELSE FALSE 
    END AS succeeded, 
    data :transaction :message :accountKeys :: ARRAY AS account_keys, 
    data :meta :preBalances :: ARRAY AS pre_balances, 
    data :meta :postBalances :: ARRAY AS post_balances, 
    data :meta :preTokenBalances :: ARRAY AS pre_token_balances, 
    data :meta :postTokenBalances :: ARRAY AS post_token_balances, 
    data :transaction :message :instructions :: ARRAY AS instructions, 
    data :meta :innerInstructions :: ARRAY AS inner_instructions, 
    data :meta :logMessages :: ARRAY as log_messages,
    b._inserted_date, 
    _partition_id
FROM 
    {{ ref('bronze__transactions2') }} t

LEFT OUTER JOIN {{ ref('silver__blocks2') }} b
ON t.block_id = b.block_id
  
WHERE 
    _partition_id >= 2200
    ---AND t._partition_id <= 1499
    AND (error IS NULL 
    OR error :code <> '-32009') -- block is empty
    AND 
        COALESCE(
            data :transaction :message :instructions [0] :programId :: STRING,
            ''
        ) <> 'Vote111111111111111111111111111111111111111'

{% if is_incremental() %}
AND
  _partition_id >= (
    SELECT
      MAX(_partition_id)
    FROM
      max_partition
  )
{% endif %}
