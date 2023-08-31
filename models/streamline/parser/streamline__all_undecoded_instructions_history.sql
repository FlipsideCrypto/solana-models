{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_program_parser(object_construct('realtime', 'False'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

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
    JOIN {{ ref('streamline__idls_history_pointer') }}
    p
    ON e.block_timestamp >= p.backfill_to_date
    AND e.block_timestamp <= p.min_decoded_block_timestamp_date
    AND e.program_id = p.program_id
