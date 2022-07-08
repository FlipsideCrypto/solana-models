{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH tx_base AS (
    SELECT 
        data :result :slot AS block_id, 
        TO_TIMESTAMP_NTZ(data :result :blockTime) AS block_timestamp, 
        tx_id, 
        CASE WHEN data :result :meta :err :: STRING IS NULL 
            THEN true
        ELSE 
            false
        END AS succeeded,     
        data :result :transaction :signatures :: ARRAY AS signers, 
        data :result :transaction :message :accountKeys AS account_keys,
        data :result :transaction :message :instructions AS instruction, 
        data :result :meta :innerInstructions AS inner_instruction, 
        data :result :meta :preBalances AS pre_balances, 
        data :result :meta :postBalances AS post_balances, 
        data :result :meta :preTokenBalances AS pre_token_balances, 
        data :result :meta :postTokenBalances AS post_token_balances
    FROM  
        {{ source(
            'solana_external', 
            'txs_api') 
        }} 
), 

instructs AS (
    SELECT 
        tx_id, 
        index, 
        i.value :parsed :type :: STRING AS event_type, 
        i.value :programId :: STRING AS program_id
    FROM 
        {{ source(
            'solana_external', 
            'txs_api') 
        }}, 
    LATERAL FLATTEN (input => data :result :transaction :message :instructions) i

)

SELECT 
    block_id, 
    block_timestamp, 
    i.tx_id, 
    succeeded, 
    index, 
    event_type, 
    program_id, 
    signers, 
    account_keys, 
    instruction, 
    inner_instruction, 
    pre_balances, 
    post_balances, 
    pre_token_balances, 
    post_token_balances, 
    NULL AS _inserted_timestamp
FROM instructs i

LEFT OUTER JOIN tx_base b
ON i.tx_id = b.tx_id