{{ config(
    materialized = 'view'
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    INDEX,
    program_id,
    decoded_instruction,
    COALESCE (
        decoded_instructions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id', 'index']
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
