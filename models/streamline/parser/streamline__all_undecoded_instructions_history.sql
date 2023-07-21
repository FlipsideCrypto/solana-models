{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_program_parser()",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH last_3_days AS ({% if var('STREAMLINE_RUN_HISTORY') %}

    SELECT
        0 AS block_id
    {% else %}
    SELECT
        MAX(block_id) - 100000 AS block_id --aprox 3 days
    FROM
        {{ ref("streamline__all_undecoded_instructions") }}
    {% endif %}),
    tbl AS (
        SELECT
            program_id,
            tx_id,
            INDEX,
            instruction,
            block_id,
            block_timestamp
        FROM
            {{ ref("streamline__all_undecoded_instructions") }}
        WHERE
            (
                block_id >= (
                    SELECT
                        block_id
                    FROM
                        last_3_days
                )
            )
            AND block_id IS NOT NULL
            AND concat_ws(
                '-',
                block_id,
                program_id,
                INDEX
            ) NOT IN (
                SELECT
                    id
                FROM
                    {{ ref("streamline__complete_decoded_instructions") }}
                WHERE
                    block_id >= (
                        SELECT
                            block_id
                        FROM
                            last_3_days
                    )
                    AND block_id IS NOT NULL
            )
    )
SELECT
    program_id,
    tx_id,
    INDEX,
    instruction,
    block_id,
    block_timestamp
FROM
    tbl
WHERE program_id = (SELECT MAX(program_id) AS program_id FROM tbl)