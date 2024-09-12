{{ config(
    materialized = 'incremental',
    unique_key = ['fact_events_inner_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id, instruction_program_id, program_id)'),
    tags = ['scheduled_non_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    signers,
    succeeded,
    instruction_index,
    inner_index,
    instruction_program_id,
    program_id,
    event_type,
    instruction,
    _inserted_timestamp,
    events_inner_id as fact_events_inner_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__events_inner') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
{% endif %}
