{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core'],
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    signers,
    INDEX,
    inner_index,
    program_id,
    event_type,
    decoded_instruction,
    decoded_instructions_combined_id AS fact_decoded_instructions_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__decoded_instructions_combined') }}
