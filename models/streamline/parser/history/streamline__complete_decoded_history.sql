{{ config (
    materialized = 'incremental',
    unique_key = 'program_id',
    tags = ['streamline'],
) }}

WITH min_decoded AS (

    SELECT
        SPLIT_PART(
            id,
            '-',
            3
        ) :: STRING AS program_id,
        MIN(block_id) AS block_id
    FROM
        {{ ref('streamline__complete_decoded_instructions') }}
    GROUP BY
        1
)
SELECT
    h.program_id
FROM
    {{ ref('streamline__idls_history') }}
    h
    JOIN min_decoded
    ON min_decoded.block_id = h.first_block_id

{% if is_incremental() %}
WHERE
    h.program_id NOT IN (
        SELECT
            DISTINCT(program_id)
        FROM
            {{ this }}
    )
{% endif %}
