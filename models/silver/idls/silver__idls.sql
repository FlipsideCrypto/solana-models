{{ config(
    materialized = 'incremental',
    unique_key = "program_id",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['idls']
) }}

WITH submitted_idls AS (

    SELECT
        A.program_id,
        A.idl,
        A.idl_hash,
        TRUE as is_valid, --all idls from verified_idls are valid
        A.discord_username,
        A._inserted_timestamp,
        a.is_active,
        a.last_activity_timestamp,
        b.first_block_id
    FROM
        {{ ref('silver__verified_idls') }} A
        LEFT JOIN {{ ref('streamline__idls_history') }}
        b
        ON A.program_id = b.program_id qualify(ROW_NUMBER() over(PARTITION BY A.program_id
    ORDER BY
        A._inserted_timestamp DESC)) = 1
),
idl_decoded_history AS (
    SELECT
        MIN(block_id) AS earliest_decoded_block,
        program_id
    FROM
        {{ ref('silver__decoded_instructions') }}

{% if is_incremental() %}
WHERE
    _inserted_timestamp >= SYSDATE() - INTERVAL '2 hours'
{% endif %}
GROUP BY
    program_id
),
idls_in_progress AS (
    SELECT
        table_schema,
        table_name,
        SPLIT_PART(
            table_name,
            '_',
            ARRAY_SIZE(SPLIT(table_name, '_'))
        ) AS in_progress_program_id
    FROM
        information_schema.views
    WHERE
        table_name LIKE 'DECODED_INSTRUCTIONS_BACKFILL_%'
        AND table_name NOT LIKE '%RETRY%'
),
pre_final AS (
    SELECT
        A.program_id,
        A.idl,
        A.idl_hash,
        A.is_valid,
        A.discord_username,
        A._inserted_timestamp,
        a.is_active,
        a.last_activity_timestamp,
        A.first_block_id,
    {% if is_incremental() %}
        iff(b.earliest_decoded_block < d.earliest_decoded_block, b.earliest_decoded_block, d.earliest_decoded_block) AS earliest_decoded_block,
    {% else %}
        b.earliest_decoded_block,
    {% endif %}
        C.in_progress_program_id
    FROM
        submitted_idls A
        LEFT JOIN idl_decoded_history b
        ON A.program_id = b.program_id
        LEFT JOIN idls_in_progress C
        ON A.program_id = C.in_progress_program_id

{% if is_incremental() %}
LEFT JOIN {{ this }}
d
ON A.program_id = d.program_id
    {% endif %}
)
SELECT
    program_id,
    idl,
    idl_hash,
    is_valid,
    is_active,
    last_activity_timestamp,
    discord_username as submitted_by,
    _inserted_timestamp as date_submitted,
    first_block_id,
    earliest_decoded_block,
    CASE
        WHEN earliest_decoded_block = first_block_id THEN 'complete' 
        WHEN in_progress_program_id IS NOT NULL THEN 'in_progress'
        WHEN NOT is_valid THEN NULL
        ELSE 'not_started'
    END AS backfill_status,
    {{ dbt_utils.generate_surrogate_key(['program_id']) }} AS idls_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id

FROM
    pre_final
