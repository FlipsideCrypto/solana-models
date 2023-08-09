{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_program_parser(object_construct('realtime', 'False'))",
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
min_completed AS (
    SELECT 
        MIN(block_id) as min_block_completed
    FROM 
        {{ ref('streamline__complete_decoded_instructions') }}
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
    JOIN 
        min_completed m
    WHERE 
        e.block_id between min_block_completed - 500000 and min_block_completed
),
completed_subset AS (
    SELECT 
        block_id,
        id 
    FROM
        {{ ref('streamline__complete_decoded_instructions') }}
    WHERE 
        block_id between (SELECT MIN(block_id) from event_subset) and (SELECT MAX(block_id) from event_subset)
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
