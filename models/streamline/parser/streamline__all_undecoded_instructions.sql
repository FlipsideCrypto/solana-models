{{ config (
    materialized = "incremental",
    unique_key = "CONCAT_WS('-', tx_id, INDEX)",
    cluster_by = "ROUND(block_id, -3)",
) }}
--  post_hook = 'call silver.sp_bulk_decode_instructions()',
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
instr_in_play AS (
    SELECT
        A.program_id,
        tx_id,
        INDEX,
        instruction,
        block_id, 
        block_timestamp
    FROM
        {{ ref('silver__events') }} A
        JOIN idl_in_play b
        ON LOWER(
            A.program_id
        ) = b.program_id
)
SELECT
    p.program_id,
    p.tx_id,
    p.index,
    p.instruction,
    p.block_id,
    p.block_timestamp
FROM
    instr_in_play p
    LEFT OUTER JOIN {{ source(
        'bronze',
        'decoded_instructions'
    ) }}
    d
    ON p.tx_id = d.tx_id
    AND p.index = d.index
    AND p.block_timestamp = d.block_timestamp
WHERE
    d.tx_id IS NULL
