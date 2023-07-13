{{ config(
    materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    INDEX,
    program_id,
    decoded_instruction
FROM
    {{ ref('silver__decoded_instructions') }}
