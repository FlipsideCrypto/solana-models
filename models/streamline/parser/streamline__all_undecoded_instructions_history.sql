{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_program_parser()",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

{% for item in range(20) %}
    (
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
            program_id = 'JUP3c2Uh3WA4Ng34tw6kPd2G4C5BB21Xo36Je1s32Ph'
            AND 
            block_id BETWEEN {{ item * 1000000 + 80000000 }}
            AND {{(
                item + 1
            ) * 1000000 + 80000000}}
        ORDER BY
            block_id
    ) {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
