{{ config(
    materialized = 'incremental',
    unique_key = ['block_id', 'tx_id', 'instruction_index', 'inner_index'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE','ROUND(block_id, -3)'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, program_id, instruction_program_id, instruction_index, inner_index, event_type)'),
    merge_exclude_columns = ["inserted_timestamp"],
    full_refresh = false,
    tags = ['scheduled_core']
) }}

WITH pre_final AS (

    SELECT
        e.block_timestamp,
        e.block_id,
        e.tx_id,
        e.succeeded,
        e.signers,
        e.index AS instruction_index,
        ii.index :: INTEGER AS inner_index,
        e.program_id AS instruction_program_id,
        ii.value :programId :: STRING AS program_id,
        ii.value :parsed :type :: STRING AS event_type,
        ii.value AS instruction,
        e._inserted_timestamp
    FROM
        {{ ref('silver__events') }}
        e,
        TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        1 = 1

{% if is_incremental() %}
{% if execute %}
    {{ get_batch_load_logic(this,5,'2024-09-17') }}
{% endif %}
{% else %}
    AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-08-15'
{% endif %}
)
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
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id', 'instruction_index', 'inner_index']
    ) }} AS events_inner_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
