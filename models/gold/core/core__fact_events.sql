{{ config(
    materialized = 'view',
    post_hook = 'ALTER VIEW {{this}} SET CHANGE_TRACKING = TRUE;',
    tags = ['scheduled_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    signers,
    succeeded,
    INDEX,
    event_type,
    program_id,
    instruction,
    inner_instruction,
    COALESCE (
        events_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id', 'tx_id', 'index']
        ) }}
    ) AS fact_events_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__events') }}
