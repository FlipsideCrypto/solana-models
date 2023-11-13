{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_program_parser(object_construct('realtime', 'retry'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline'],
) }}

SELECT
    *
FROM
    {{ ref('silver__decoded_instructions') }}
WHERE
    decoded_instruction :error IS NOT NULL
