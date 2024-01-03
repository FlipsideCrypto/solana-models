{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core'],
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    INDEX,
    inner_index,
    program_id,
    decoded_instruction,
    COALESCE (
        decoded_instructions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id', 'index', 'inner_index']
        ) }}
    ) AS fact_decoded_instructions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__decoded_instructions') }}
