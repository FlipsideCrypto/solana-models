{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_program_parser(object_construct('realtime', 'retry'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH retry AS (

    SELECT
        program_id,
        tx_id,
        INDEX,
        block_id
    FROM
        {{ ref('silver__decoded_instructions') }}
    WHERE
        decoded_instruction :args :unknown IS NOT NULL
        OR decoded_instruction IS NULL
)
SELECT
    e.program_id,
    e.tx_id,
    e.index,
    e.instruction,
    e.block_id,
    e.block_timestamp
FROM
    {{ ref('silver__events') }}
    e
    INNER JOIN retry
    ON e.program_id = retry.program_id
    AND e.tx_id = retry.tx_id
    AND e.index = retry.index
    AND e.block_id = retry.block_id
