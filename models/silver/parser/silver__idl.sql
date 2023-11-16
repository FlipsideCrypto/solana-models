{{ config(
    materialized = 'incremental',
    unique_key = "program_id",
    tags = ['scheduled_non_core']
) }}

WITH idls AS (

    SELECT
        MAX(block_id) AS max_block_id,
        MIN(block_id) AS min_block_id,
        program_id
    FROM
        {{ ref('silver__decoded_instructions') }}
    GROUP BY
        program_id
),
pre_final AS (
    SELECT
        A.max_block_id,
        A.min_block_id,
        A.program_id,
        b.first_block_id
    FROM
        idls A
        LEFT JOIN {{ ref('streamline__idls_history') }}
        b
        ON A.program_id = b.program_id
)
SELECT
    *,
    CASE
        WHEN first_block_id = min_block_id THEN 'Complete'
        ELSE 'In Progress'
    END AS status_historical_data,
    {{ dbt_utils.generate_surrogate_key(['program_id']) }} AS idl_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    pre_final
