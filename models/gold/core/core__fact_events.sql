{{ config(
    materialized = 'view',
    tags = ['scheduled_core']
) }}

SELECT 
    block_timestamp,
    block_id,
    tx_id,
    signers,
    succeeded,
    index,
    event_type,
    program_id,
    instruction,
    inner_instruction
FROM
    {{ ref('silver__events') }}
