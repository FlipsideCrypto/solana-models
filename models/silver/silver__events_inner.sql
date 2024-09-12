{{ config(
    materialized = 'incremental',
    unique_key = ['events_inner_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE','program_id'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, program_id, event_type, inner_instruction_program_ids)'),
    full_refresh = false,
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['events_inner_backfill']
) }}



with pre_final as (

select 
    e.block_timestamp,
    e.block_id,
    e.tx_id,
    e.succeeded,
    e.signers,
    e.index as instruction_index,
    ii.index::integer as inner_index,
    e.program_id as instruction_program_id,
    ii.value:programId :: STRING AS program_id,
    ii.value:parsed:type::string as event_type,
    ii.value AS instruction,
    e._inserted_timestamp
from {{ ref('silver__events') }} e,
    TABLE(FLATTEN(inner_instruction :instructions)) ii
    WHERE
        1 = 1
{% if is_incremental() %}
{% if execute %}
    {{ get_batch_load_logic( this, 15, '2024-09-10') }}
{% endif %}
{% else %}
    -- AND _inserted_timestamp :: DATE BETWEEN '2022-08-12' AND '2022-09-01'
    and _inserted_timestamp :: DATE = '2024-06-03'

    
{% endif %}
    )

select 
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
from pre_final

