{{ config(
    materialized = 'view'
) }}

SELECT
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.signers,
    b.succeeded,
    b.index,
    b.event_type,
    b.program_id,
    b.instruction,
    A.decoded_instruction
FROM
    {{ ref('silver__decoded_instructions') }} A
    JOIN {{ ref('silver__events') }}
    b
    ON A.program_id = b.program_id
    AND A.tx_id = b.tx_id
    AND A.index = b.index
