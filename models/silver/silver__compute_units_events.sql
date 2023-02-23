{{ config(
    materialized = 'incremental',
    unique_key = "CONCAT_WS('-', tx_id, index)",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE']
) }}

WITH base AS ( 
    SELECT 
        -- Are these the columns we want to include?
        block_timestamp, 
        block_id, 
        tx_id, 
        signers, 
        succeeded,  
        instructions, 
        inner_instructions, 
        log_messages, 
        c.value :: STRING as compute_str, 
        index, 
        _inserted_timestamp
    FROM 
        {{ ref('silver__transactions') }} t, 
    LATERAL FLATTEN (
        input => log_messages
    ) c
    WHERE c.value LIKE 'Program%consumed%of%compute units'
    {% if is_incremental() %}
        {% if execute %}
        {{ get_batch_load_logic(this,30,'2023-02-23') }}
        {% endif %}
    {% else %}
        AND _inserted_timestamp::date between '2022-08-30' and '2022-09-05'
    {% endif %} 
    --AND tx_id = '5CZ9kgU1aiEmifq8DaC1gLfNmq1NnQYEJbYAZwrMQNriwvMyjaoqwPxNG7bGYExD756xLcVjozFhPhbCuhj992yn' -- Kellen's Transaction ID
), 
string_manipulation AS (
    SELECT 
        block_timestamp,
        block_id, 
        tx_id, 
        signers, 
        succeeded, 
        instructions, 
        inner_instructions,  
        RIGHT(compute_str, LEN(compute_str) - POSITION('Program ', compute_str) - 8) AS program_tmp, 
        LEFT(program_tmp, POSITION(' consumed', program_tmp)) AS program_id,
        RIGHT(compute_str, LEN(compute_str) - POSITION('consumed', compute_str) - 8) AS units_consumed_str,
        RIGHT(compute_str, LEN(compute_str) - POSITION(' of ', compute_str) - 3 ) AS unit_limit_str,
        LEFT(units_consumed_str, POSITION(' ', units_consumed_str)) :: INT AS units_consumed,
        LEFT(unit_limit_str, POSITION(' ', unit_limit_str)) :: INT AS unit_limit, 
        index, 
        _inserted_timestamp
    FROM base
)
SELECT 
    block_timestamp, 
    block_id, 
    tx_id, 
    signers, 
    succeeded, 
    index, 
    program_id, 
    units_consumed, 
    unit_limit,  
    instructions, 
    inner_instructions, 
    _inserted_timestamp
FROM string_manipulation 