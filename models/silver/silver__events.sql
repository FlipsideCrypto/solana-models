{{ config(
    materialized = 'incremental',
    unique_key = ['block_id','tx_id','index'],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::date >= LEAST(current_date-7,(select min(block_timestamp)::date from ' ~ generate_tmp_view_name(this) ~ '))'],
    cluster_by = ['block_timestamp::DATE','_inserted_timestamp::DATE','program_id'],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}'), 
    full_refresh = false,
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_core']
) }}

WITH base_i AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INDEX :: INTEGER AS INDEX,
        VALUE :parsed :type AS event_type,
        VALUE :programId AS program_id,
        VALUE,
        _inserted_timestamp
    FROM
        {{ ref('silver___instructions') }}
        i

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    i.block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368) + 1, 153013616)
        FROM
            {{ this }}
)
AND (
    SELECT
        LEAST(COALESCE(MAX(block_id), 105368) + 4000000, 153013616)
    FROM
        {{ this }}
) 
{% elif is_incremental() %}
    WHERE
        _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp)
    FROM
        {{ this }}
    )
{% else %}
    WHERE
        i.block_id BETWEEN 105368
        AND 1000000
{% endif %}
),
base_ii AS (
    SELECT
        block_id,
        tx_id,
        mapped_instruction_index :: INTEGER AS mapped_instruction_index,
        VALUE,
        silver.udf_get_all_inner_instruction_program_ids(VALUE) AS inner_instruction_program_ids,
        _inserted_timestamp
    FROM
        {{ ref('silver___inner_instructions') }}
        ii

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    ii.block_id BETWEEN (
        SELECT
            LEAST(COALESCE(MAX(block_id), 105368) + 1, 153013616)
        FROM
            {{ this }})
        AND (
            SELECT
                LEAST(COALESCE(MAX(block_id), 105368) + 4000000, 153013616)
            FROM
                {{ this }}
    ) 
{% elif is_incremental() %}
    WHERE
        _inserted_timestamp >= (
            SELECT
                MAX(_inserted_timestamp)
            FROM
                {{ this }}
    )
{% else %}
    WHERE
        ii.block_id BETWEEN 105368
        AND 1000000
{% endif %}
)
SELECT
    i.block_timestamp,
    i.block_id,
    i.tx_id,
    t.signers,
    i.succeeded,
    i.index,
    i.event_type :: STRING AS event_type,
    i.program_id :: STRING AS program_id,
    i.value AS instruction,
    ii.value AS inner_instruction,
    ii.inner_instruction_program_ids,
    i._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['i.block_id', 'i.tx_id', 'i.index']
    ) }} AS events_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base_i i
LEFT OUTER JOIN base_ii ii
ON ii.block_id = i.block_id
AND ii.tx_id = i.tx_id
AND ii.mapped_instruction_index = i.index
LEFT OUTER JOIN {{ ref('silver__transactions') }}
t
ON i.block_id = t.block_id
AND i.tx_id = t.tx_id

{% if is_incremental() and env_var(
    'DBT_IS_BATCH_LOAD',
    "false"
) == "true" %}
WHERE
    t.block_id BETWEEN (
SELECT
    LEAST(COALESCE(MAX(block_id), 105368) + 1, 153013616)
FROM
    {{ this }})
AND (
SELECT
    LEAST(COALESCE(MAX(block_id), 105368) + 4000000, 153013616)
FROM
    {{ this }}) 
{% elif is_incremental() %}
WHERE
    t._inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp)
        FROM
            {{ this }}
        )
{% else %}
WHERE
    t.block_id BETWEEN 105368
    AND 1000000
{% endif %}
