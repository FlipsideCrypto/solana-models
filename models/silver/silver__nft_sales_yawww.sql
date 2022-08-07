{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH base_table AS (
    SELECT 
        e.block_timestamp, 
        e.block_id, 
        e.tx_id, 
        t.succeeded, 
        e.program_id, 
        CASE WHEN t.log_messages[1] :: STRING LIKE 'Program log: Instruction: Accept bid' THEN 
            instruction :accounts[1] :: STRING 
        ELSE
            instruction :accounts[0] :: STRING 
        END AS purchaser,  
        CASE WHEN t.log_messages[1] :: STRING LIKE 'Program log: Instruction: Accept bid' THEN
            instruction :accounts[10] :: STRING 
        ELSE 
            instruction :accounts[9] :: STRING
        END AS acct_1, 
        instruction :accounts[8] :: STRING AS mint, 
        CASE WHEN t.log_messages[1] :: STRING LIKE 'Program log: Instruction: Accept bid' THEN
            'bid'
        ELSE
            'direct buy'
        END AS sale_type, 
        e._inserted_timestamp, 
        t.log_messages AS _log_messages
    FROM {{ ref('silver__events') }} 
    e
    
    INNER JOIN {{ ref('silver__transactions') }} t
    ON t.tx_id = e.tx_id 
  
    WHERE 
        program_id = '5SKmrbAxnHV2sgqyDXkGrLrokZYtWWVEEk5Soed7VLVN' -- yawww program ID

    {% if is_incremental() %}
    AND e._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    AND t._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
),  
price_buys AS (
    SELECT
        b.tx_id,
        SUM(i.value :parsed :info :lamports) / POW(10, 9) :: NUMBER AS sales_amount -- sales amount, but only for buys
    FROM
        base_table 
        b
        
        INNER JOIN {{ ref('silver__events') }}  e
        ON e.tx_id = b.tx_id
        
        LEFT JOIN TABLE(FLATTEN(inner_instruction :instructions)) i
       
    WHERE
        i.value :parsed :type :: STRING = 'transfer'
        AND i.value :program :: STRING = 'system'

    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
    GROUP BY b.tx_id 
),  
sellers AS (
    SELECT
        i.value :accounts[0] :: STRING AS seller,
        i.value :accounts[5] :: STRING AS acct_1
    FROM {{ ref('silver__transactions') }} 
    t
    LEFT JOIN TABLE(FLATTEN(instructions)) i
    WHERE 
        i.value :programId = '5SKmrbAxnHV2sgqyDXkGrLrokZYtWWVEEk5Soed7VLVN' -- yawww program ID
        AND log_messages[3] :: STRING LIKE 'Program log: Instruction: List item'
    
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
),  
price_bids AS (
    SELECT 
        signers[0] :: STRING AS purchaser,
        index, 
        i.value :parsed :info :lamports / POW(10, 9) AS bid_amount
    FROM {{ ref('silver__transactions') }} 
    t
    LEFT JOIN TABLE(FLATTEN(inner_instructions[0] :instructions)) i
  
    WHERE 
        log_messages[1] :: STRING LIKE 'Program log: Instruction: Bid on listing'
        AND i.value :parsed :type :: STRING = 'transfer'
        AND index = 3 
    
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
     b.block_timestamp, 
     b.block_id, 
     b.tx_id, 
     b.succeeded, 
     b.program_id, 
     b.mint, 
     b.purchaser, 
     s.seller, 
     COALESCE(
       sales_amount,
       bid_amount
     ) AS sales_amount, 
     sale_type,  
     b._inserted_timestamp
FROM base_table b

LEFT OUTER JOIN price_buys p
ON b.tx_id = p.tx_id

LEFT OUTER JOIN sellers s
ON b.acct_1 = s.acct_1

LEFT OUTER JOIN price_bids bd
ON b.purchaser = bd.purchaser