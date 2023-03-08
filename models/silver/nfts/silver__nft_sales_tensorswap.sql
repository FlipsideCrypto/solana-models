{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, mint, purchaser)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

SELECT
    block_id, 
    block_timestamp, 
    tx_id, 
    succeeded,
    i.value:programId :: STRING AS program_id, 
    i.value:accounts[4] :: STRING AS mint, 
    i.value:accounts[8] :: STRING AS purchaser, 
    i.value:accounts[7] :: STRING AS seller,
    MAX(ABS(post_balances[keys.index] - pre_balances[keys.index]) / POWER(10,9)) AS sales_amount, 
    _inserted_timestamp
FROM 
    {{ ref('silver__transactions') }}
INNER JOIN lateral flatten (input => instructions) i
INNER JOIN lateral flatten (input => account_keys) keys
WHERE 
    array_contains('Program log: Instruction: BuySingleListing'::VARIANT, log_messages)
    AND i.value:programId = 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN'
    AND i.value:accounts[8] = signers[0]
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
        )
    {% else %}
    AND block_timestamp :: date >= '2022-09-22'
    {% endif %}
GROUP BY
    block_id, 
    block_timestamp,
    tx_id,
    succeeded,
    i.value, 
    i.value:accounts[8], 
    i.value:accounts[7],
    i.value:accounts[4], 
    _inserted_timestamp

UNION 

SELECT
    block_id, 
    block_timestamp, 
    tx_id, 
    succeeded,
    i.value:programId :: STRING AS program_id, 
    i.value:accounts[5] :: STRING AS mint,
    i.value:accounts[11] :: STRING AS purchaser, 
    i.value:accounts[10] :: STRING AS seller,
    MAX(ABS(post_balances[keys.index] - pre_balances[keys.index]) / POWER(10,9)) AS sales_amount, 
    _inserted_timestamp
FROM 
    {{ ref('silver__transactions') }}
INNER JOIN lateral flatten (input => instructions) i
INNER JOIN lateral flatten (input => account_keys) keys
WHERE 
    array_contains('Program log: Instruction: BuyNft'::VARIANT, log_messages)
    AND i.value:programId = 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN'
    AND i.value:accounts[11] = signers[0]
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% else %}
    AND block_timestamp :: date >= '2022-09-22'
    {% endif %}
GROUP BY
    block_id, 
    block_timestamp,
    tx_id,
    succeeded,
    i.value, 
    i.value:accounts[11], 
    i.value:accounts[10],
    i.value:accounts[5], 
    _inserted_timestamp

UNION 

SELECT 
    block_id, 
    block_timestamp, 
    tx_id, 
    succeeded,
    i.value:programId :: STRING AS program_id, 
    i.value:accounts[6] :: STRING AS mint, 
    i.value:accounts[9] :: STRING AS purchaser, 
    i.value:accounts[10] :: STRING AS seller,
    MAX(ABS(post_balances[keys.index] - pre_balances[keys.index]) / POWER(10,9)) AS sales_amount, 
    _inserted_timestamp
FROM 
    {{ ref('silver__transactions') }}
INNER JOIN lateral flatten (input => instructions) i
INNER JOIN lateral flatten (input => account_keys) keys
WHERE 
    array_contains('Program log: Instruction: SellNftTokenPool'::VARIANT, log_messages)
    AND i.value:programId = 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN'
    AND i.value:accounts[10] = signers[0]
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% else %}
    AND block_timestamp :: date >= '2022-09-22'
    {% endif %}
GROUP BY
    block_id, 
    block_timestamp,
    tx_id,
    succeeded,
    i.value,
    i.value:accounts[9], 
    i.value:accounts[10],
    i.value:accounts[6], 
    _inserted_timestamp

UNION 

SELECT 
    block_id, 
    block_timestamp, 
    tx_id, 
    succeeded,
    i.value:programId :: STRING AS program_id, 
    i.value:accounts[6] :: STRING AS mint,
    i.value:accounts[9] :: STRING AS purchaser, 
    i.value:accounts[10] :: STRING AS seller,
    MAX(ABS(post_balances[keys.index] - pre_balances[keys.index]) / POWER(10,9)) AS sales_amount, 
    _inserted_timestamp
FROM 
    {{ ref('silver__transactions') }}
INNER JOIN lateral flatten (input => instructions) i
INNER JOIN lateral flatten (input => account_keys) keys
WHERE 
    array_contains('Program log: Instruction: SellNftTradePool'::VARIANT, log_messages)
    AND i.value:programId = 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN'
    AND i.value:accounts[10] = signers[0]
    {% if is_incremental() %}
    AND _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
    )
    {% else %}
    AND block_timestamp :: date >= '2022-09-22'
    {% endif %}
GROUP BY
    block_id, 
    block_timestamp,
    tx_id,
    succeeded,
    i.value,
    i.value:accounts[9], 
    i.value:accounts[10],
    i.value:accounts[6], 
    _inserted_timestamp

