{{ config(
    materialized = 'incremental',
    unique_key = ['ez_events_decoded_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','ROUND(block_id, -3)','program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id, event_type)'),
    full_refresh = false,
    tags = ['scheduled_non_core']
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
    inner_index,
    event_type,
    program_id,
    decoded_instruction,
    decoded_instruction :accounts :: ARRAY AS decoded_accounts,
    decoded_instruction :args :: variant AS decoded_args,
    decoded_instruction :error :: STRING AS decoding_error,
    decoded_instructions_combined_id AS ez_events_decoded_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM
    {{ ref('silver__decoded_instructions_combined') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}


