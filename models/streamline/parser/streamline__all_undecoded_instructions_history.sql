{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_program_parser()",
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
)
, min_completed AS (
    SELECT 
        MIN(block_id) as min_block_completed
    FROM 
        {{ ref('streamline__complete_decoded_instructions') }}
)
SELECT
    A.program_id,
    A.tx_id,
    A.INDEX,
    A.instruction,
    A.block_id, 
    A.block_timestamp
FROM
    {{ ref('silver__events') }} A
    JOIN idl_in_play b
    ON LOWER(
        A.program_id
    ) = b.program_id
    LEFT OUTER JOIN {{ ref('streamline__complete_decoded_instructions') }} c 
    ON c.block_id = A.block_id
    AND concat_ws(
            '-',
            A.block_id,
            A.program_id,
            A.INDEX
        ) = c.id
    JOIN min_completed m
WHERE 
    c.block_id IS NULL
AND 
    A.block_id between min_block_completed - 10000000 and min_block_completed
