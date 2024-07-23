{{ config (
    materialized = "table",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_instructions_decoder(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'decoded_instructions_2', 'sql_limit', {{var('sql_limit','5000000')}}, 'producer_batch_size', {{var('producer_batch_size','2000000')}}, 'worker_batch_size', {{var('worker_batch_size','100000')}}, 'batch_call_limit', {{var('batch_call_limit','1000')}}, 'call_type', 'RT'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_decoder']
) }}

{% if execute %}
    {% set min_event_block_id_query %}
        select min(block_id)
        from {{ ref('silver__events') }}
        where 
            block_timestamp >= CURRENT_DATE - 2
    {% endset %}
    {% set min_event_block_id = run_query(min_event_block_id_query).columns[0].values()[0] %}
{% endif %}

WITH idl_in_play AS (

    SELECT
        program_id
    FROM
        {{ ref('silver__verified_idls') }}
    WHERE   
        program_id <> 'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH'
),
event_subset AS (
    SELECT
        e.program_id,
        e.tx_id,
        e.index,
        NULL as inner_index,
        e.instruction,
        e.block_id,
        e.block_timestamp,
        {{ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.index','inner_index','e.program_id']) }} as id
    FROM
        {{ ref('silver__events') }}
        e
        JOIN idl_in_play b
        ON e.program_id = b.program_id
    WHERE
        e.block_timestamp >= CURRENT_DATE - 2
    AND 
        e.succeeded
    UNION ALL
    SELECT
        i.value :programId :: STRING AS inner_program_id,
        e.tx_id,
        e.index,
        i.index AS inner_index,
        i.value AS instruction,
        e.block_id,
        e.block_timestamp,
        {{ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.index','inner_index','inner_program_id']) }} as id
    FROM
        {{ ref('silver__events') }}
        e
        JOIN idl_in_play b
        ON ARRAY_CONTAINS(b.program_id::variant, e.inner_instruction_program_ids) 
        JOIN table(flatten(e.inner_instruction:instructions)) i 
    WHERE
        e.block_timestamp >= CURRENT_DATE - 2
    AND 
        e.succeeded
    AND 
        i.value :programId :: STRING = b.program_id
    AND (
            (
                inner_program_id in ('FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn','SNPRohhBurQwrpwAptw1QYtpFdfEKitr4WSJ125cN1g','GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY','JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4','DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M','PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu','LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo','PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY')
                AND array_size(i.value:accounts) > 1
            )
            OR inner_program_id not in ('FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn','SNPRohhBurQwrpwAptw1QYtpFdfEKitr4WSJ125cN1g','GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY','JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4','DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M','PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu','LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo','PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY')
        )
),
completed_subset AS (
    SELECT
        block_id,
        complete_decoded_instructions_2_id as id
    FROM
        {{ ref('streamline__complete_decoded_instructions_2') }}
    WHERE
        block_id >= {{ min_event_block_id }} --ensure we at least prune to last 2 days worth of blocks since the dynamic below will scan everything
    AND block_id >= (
            SELECT
                MIN(block_id)
            FROM
                event_subset
        )
)
SELECT
    e.program_id,
    e.tx_id,
    e.index,
    e.inner_index,
    e.instruction,
    e.block_id,
    e.block_timestamp
FROM
    event_subset e
    LEFT OUTER JOIN completed_subset C
    ON C.block_id = e.block_id
    AND e.id = C.id
WHERE
    C.block_id IS NULL