{{ config(
    materialized = 'incremental',
    unique_key = "program_id",
    tags = ['scheduled_non_core']
) }}

WITH idls AS (

    SELECT
        MIN(block_id) AS earliest_decoded_block,
        program_id
    FROM
        {{ ref('silver__decoded_instructions') }}
    GROUP BY
        program_id
),
pre_final AS (
    SELECT
        A.earliest_decoded_block,
        A.program_id,
        C.idl,
        C.idl_source,
        C.idl_hash
    FROM
        idls A
        LEFT JOIN {{ ref('streamline__idls_history') }}
        b
        ON A.program_id = b.program_id
        LEFT JOIN {{ ref('silver__verified_idls') }} C
        ON A.program_id = C.program_id
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(['program_id']) }} AS idl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final
