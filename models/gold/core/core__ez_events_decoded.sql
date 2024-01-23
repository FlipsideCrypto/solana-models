{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core'],
) }}

SELECT
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.signers,
    b.succeeded,
    b.index,
    A.inner_index,
    b.event_type,
    b.program_id,
    b.instruction,
    A.decoded_instruction,
    COALESCE (
        decoded_instructions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['b.tx_id', 'b.index', 'A.inner_index']
        ) }}
    ) AS ez_events_decoded_id,
    GREATEST(COALESCE(A.inserted_timestamp, '2000-01-01'), COALESCE(b.inserted_timestamp, '2000-01-01')) AS inserted_timestamp,
    GREATEST(COALESCE(A.modified_timestamp, '2000-01-01'), COALESCE(b.modified_timestamp, '2000-01-01')) AS modified_timestamp
FROM
    {{ ref('silver__decoded_instructions') }} A
    JOIN {{ ref('silver__events') }}
    b
    ON A.block_timestamp::date = b.block_timestamp::date
    AND A.program_id = b.program_id
    AND A.tx_id = b.tx_id
    AND A.index = b.index
