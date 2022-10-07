{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH base_table AS (
    SELECT 
        block_timestamp, 
        block_id, 
        tx_id, 
        succeeded, 
        program_id, 
        instruction :accounts[0] :: STRING AS acct_1, 
        instruction :accounts[3] :: STRING AS seller, 
        instruction :accounts[1] :: STRING AS mint, 
        ingested_at, 
        _inserted_timestamp
    FROM {{ ref('silver__events') }}
    WHERE program_id = 'J7RagMKwSD5zJSbRQZU56ypHUtux8LRDkUpAPSKH4WPp' -- solana monke business marketplace

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)

{% else %}
AND 
    block_timestamp :: date >= '2022-08-17'

{% endif %}
),
price AS (
    SELECT
        b.tx_id,
        e.instruction :parsed :info :lamports :: NUMBER AS amount
    FROM
        {{ ref('silver__events') }}
        e
        INNER JOIN base_table b
        ON e.tx_id = b.tx_id
    WHERE
        e.event_type = 'transfer'

   {% if is_incremental() %}
AND e._inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
)
{% else %}
AND 
    e.block_timestamp :: date >= '2022-08-17'
{% endif %}
) 

SELECT 
     b.block_timestamp, 
     b.block_id, 
     b.tx_id, 
     b.succeeded, 
     b.program_id, 
     b.mint, 
     b.acct_1 AS purchaser, 
     b.seller, 
     p.amount / POW(10,9) AS sales_amount, 
     b.ingested_at, 
     b._inserted_timestamp
FROM base_table b

INNER JOIN price p
ON b.tx_id = p.tx_id

WHERE p.amount <> 0 -- To ignore internal wallet transfers on the marketplace
AND b.mint <> 'So11111111111111111111111111111111111111112'
