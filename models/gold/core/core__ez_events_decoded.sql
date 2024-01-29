{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core'],
) }}

SELECT
    A.block_timestamp,
    A.block_id,
    A.tx_id,
    A.signers,
    TRUE AS succeeded,
    A.index,
    A.inner_index,
    A.event_type,
    A.program_id,
    NULL AS instruction,
    A.decoded_instruction,
    A.decoded_instruction :accounts :: ARRAY AS decoded_accounts,
    A.decoded_instruction :args :: variant AS decoded_args,
    A.decoded_instruction :error :: STRING AS decoding_error,
    COALESCE (
        decoded_instructions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['a.tx_id', 'a.index', 'A.inner_index']
        ) }}
    ) AS ez_events_decoded_id,
    COALESCE(
        A.inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        A.modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__decoded_instructions') }} A
