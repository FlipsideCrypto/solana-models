{{ config(
    materialized = 'incremental',
    unique_key = ['block_id', 'tx_id', 'instruction_index', 'inner_index'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','ROUND(block_id, -3)','program_id', 'instruction_program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id, program_id, instruction_program_id, instruction_index, inner_index, event_type)'),
    full_refresh = false,
    tags = ['scheduled_core']
) }}

{% if execute %}
    {% if is_incremental() %}
    {% set max_modified_query %}
    SELECT
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ this }}
    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query)[0][0] %}
    {% endif %}
{% endif %}

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
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}