{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', block_id, tx_id)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
) }}

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
    b._inserted_date
FROM 
    {{ source(
        'solana_external', 
        'block_txs_api'
    ) }} t

LEFT OUTER JOIN {{ ref('silver__blocks2') }} b
ON t.block_id = b.block_id
  
WHERE 
    b.block_timestamp >= '2022-08-10'
    AND 
        COALESCE(
            data :transaction :message :instructions [0] :programId :: STRING,
            ''
        ) <> 'Vote111111111111111111111111111111111111111'

    qualify(ROW_NUMBER() over(PARTITION BY t.block_id, t.tx_id
ORDER BY
    b._inserted_date DESC)) = 1