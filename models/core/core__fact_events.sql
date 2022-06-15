{{ config(
    materialized = 'view'
) }}

SELECT 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    event_type,
    program_id,
    instruction,
    inner_instruction
FROM
    {{ ref('silver__events') }}
