{{ config(
    materialized = 'incremental',
    unique_key = "program_id",
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['scheduled_non_core']
) }}

WITH idls AS (

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
pre_final AS (
    SELECT
        A.earliest_decoded_block,
        A.program_id,
        b.idl,
        b.idl_hash
    FROM
        idls A
        INNER JOIN {{ ref('silver__verified_idls') }}
        b
        ON A.program_id = b.program_id

{% if is_incremental() %}
LEFT JOIN {{ this }} C
ON A.program_id = C.program_id
WHERE
    A.earliest_decoded_block < C.earliest_decoded_block
    OR C.earliest_decoded_block IS NULL
{% endif %}
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['program_id']) }} AS idl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final