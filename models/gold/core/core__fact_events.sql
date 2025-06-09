{{ config(
    materialized = 'incremental',
    unique_key = ['fact_events_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','ROUND(block_id, -3)','program_id','succeeded'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id, index, event_type)'),
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
    index,
    event_type,
    program_id,
    instruction,
    inner_instruction,
    events_id AS fact_events_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM
    {{ ref('silver__events') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
