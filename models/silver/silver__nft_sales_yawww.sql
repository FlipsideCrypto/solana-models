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
            NULL
        ELSE
            instruction :accounts[1] :: STRING 
        END AS seller,  
        CASE WHEN t.log_messages[1] :: STRING LIKE 'Program log: Instruction: Accept bid' THEN
            instruction :accounts[10] :: STRING 
        ELSE 
            instruction :accounts[9] :: STRING
        END AS acct_1,
        instruction :accounts[2] :: STRING as acct_2, 
        CASE WHEN t.log_messages[1] :: STRING LIKE 'Program log: Instruction: Accept bid' THEN 
            instruction :accounts[8] :: STRING 
        ELSE 
            instruction :accounts[7] :: STRING 
        END AS mint, 
        CASE WHEN t.log_messages[1] :: STRING LIKE 'Program log: Instruction: Accept bid' THEN
            'bid'
        ELSE
            'direct buy'
        END AS sale_type, 
        l.value :: STRING as log_messages,
        e._inserted_timestamp
    FROM {{ ref('silver__events') }} 
    e
    
    INNER JOIN "SOLANA_DEV"."SILVER"."TRANSACTIONS" t
    ON t.tx_id = e.tx_id 
  
    LEFT JOIN TABLE(FLATTEN(t.log_messages)) l
  
    WHERE 
        program_id = '5SKmrbAxnHV2sgqyDXkGrLrokZYtWWVEEk5Soed7VLVN' -- yawww program ID
        AND (l.value :: STRING ilike 'Program log: Instruction: Accept bid'
        OR l.value :: STRING ilike 'Program log: Instruction: Buy listed item')

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
    AND e._inserted_timestamp >= (
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
  
    LEFT JOIN TABLE (FLATTEN(log_messages)) l
    
    WHERE 
        i.value :programId = '5SKmrbAxnHV2sgqyDXkGrLrokZYtWWVEEk5Soed7VLVN' -- yawww program ID
        AND l.value :: STRING ilike 'Program log: Transferring user token to listing escrow...'
    
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
        instructions[0] :accounts[2] :: STRING as acct_2, 
        max(i.value :parsed :info :lamports / POW(10, 9)) AS bid_amount
    FROM {{ ref('silver__transactions') }} 
    t
    LEFT JOIN TABLE(FLATTEN(inner_instructions[0] :instructions)) i
    
    LEFT JOIN TABLE(FLATTEN(t.log_messages)) l
  
    WHERE 
        l.value :: STRING LIKE 'Program log: Instruction: Bid on listing'
        AND i.index = 3
        AND i.value :parsed :type :: STRING = 'transfer'
    
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}  

    GROUP BY 
        signers[0] :: STRING, 
        instructions[0] :accounts[2] :: STRING
)

SELECT 
     b.block_timestamp, 
     b.block_id, 
     b.tx_id, 
     b.succeeded, 
     b.program_id, 
     b.mint, 
     b.purchaser, 
     COALESCE(
        b.seller, 
        s.seller
     ) AS seller,  
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
ON b.acct_2 = bd.acct_2
AND b.purchaser = bd.purchaser