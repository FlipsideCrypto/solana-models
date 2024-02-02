{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core'],
) }}

SELECT
    A.block_timestamp,
    A.block_id,
    A.tx_id,
    A.signers,
    A.succeeded,
    A.index,
    A.inner_index,
    A.event_type,
    A.program_id,
    NULL AS instruction,
    A.decoded_instruction,
    A.decoded_instruction :accounts :: ARRAY AS decoded_accounts,
    A.decoded_instruction :args :: variant AS decoded_args,
    A.decoded_instruction :error :: STRING AS decoding_error,
    A.decoded_instructions_combined_id AS ez_events_decoded_id,
    A.inserted_timestamp,
    A.modified_timestamp
FROM
    {{ ref('silver__decoded_instructions_combined') }} A
