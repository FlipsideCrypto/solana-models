{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH instructs AS (
    SELECT 
        tx_id, 
        index, 
        i.value :parsed :type :: STRING AS event_type, 
        i.value :programId :: STRING AS program_id, 
        i.value AS instruction
    FROM 
        {{ source(
            'solana_external', 
            'txs_api') 
        }}, 
    TABLE(FLATTEN (data :result :transaction :message :instructions)) i 

    WHERE program_id = 'Stake11111111111111111111111111111111111111'

    UNION 
    SELECT
        tx_id, 
        CONCAT( b.value:index, '.', c.index ) AS INDEX, 
        c.value :parsed :type :: STRING AS event_type, 
        c.value :programId :: STRING AS program_id, 
        c.value AS instruction 
    FROM 
        {{ source(
            'solana_external', 
            'txs_api') 
        }} a, 
    TABLE(FLATTEN (data :result :meta :innerInstructions)) b, 
    TABLE(FLATTEN(b.value:instructions)) c

    WHERE program_id = 'Stake11111111111111111111111111111111111111'
),

txs AS (
    SELECT
        DISTINCT tx_id
    FROM instructs
), 

tx_base AS (
    SELECT 
        data :result :slot AS block_id, 
        TO_TIMESTAMP_NTZ(data :result :blockTime) AS block_timestamp, 
        i.tx_id, 
        CASE WHEN data :result :meta :err :: STRING IS NULL 
            THEN true
        ELSE 
            false
        END AS succeeded,     
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
        }} t

    INNER JOIN txs i 
    ON i.tx_id = t.tx_id
) 

SELECT 
    block_id, 
    block_timestamp, 
    b.tx_id, 
    succeeded, 
    index, 
    event_type, 
    program_id, 
    [] :: ARRAY AS signers, 
    account_keys :: ARRAY AS account_keys, 
    i.instruction, 
    inner_instruction :: VARIANT AS inner_instruction, 
    pre_balances :: ARRAY AS pre_balances, 
    post_balances :: ARRAY AS post_balances, 
    pre_token_balances :: ARRAY AS pre_token_balances, 
    post_token_balances :: ARRAY AS post_token_balances, 
    NULL AS _inserted_timestamp
FROM instructs i

LEFT OUTER JOIN tx_base b
ON b.tx_id = i.tx_id
