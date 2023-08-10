{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_program_parser(object_construct('realtime', 'True'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH idl_in_play AS (

    SELECT
        LOWER(
            REPLACE(SPLIT_PART(metadata$filename, '/', 3), '.json')
        ) AS program_id
    FROM
        {{ source(
            'bronze_streamline',
            'decode_instructions_idls'
        ) }}
),
event_subset AS (
    SELECT 
        e.program_id,
        e.tx_id,
        e.INDEX,
        e.instruction,
        e.block_id, 
        e.block_timestamp
    FROM 
        {{ ref('silver__events') }} e
    JOIN 
        idl_in_play b ON LOWER(e.program_id) = b.program_id
    WHERE 
        e.block_timestamp >= current_date - 2
),
completed_subset AS (
    SELECT 
        block_id,
        id 
    FROM
        {{ ref('streamline__complete_decoded_instructions') }}
    WHERE 
        block_id >= (SELECT MIN(block_id) from event_subset)
)
SELECT
    e.program_id,
    e.tx_id,
    e.INDEX,
    e.instruction,
    e.block_id, 
    e.block_timestamp
FROM
    event_subset e
    LEFT OUTER JOIN completed_subset c 
    ON c.block_id = e.block_id
    AND concat_ws(
            '-',
            e.block_id,
            e.tx_id,
            e.program_id,
            e.INDEX
        ) = c.id
WHERE 
    c.block_id is null