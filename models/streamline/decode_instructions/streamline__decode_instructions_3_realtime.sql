-- depends_on: {{ ref('silver__verified_idls') }}

{{ config (
    materialized = "table",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_instructions_decoder_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'decoded_instructions_3', 'sql_limit', {{var('sql_limit','5000000')}}, 'producer_batch_size', {{var('producer_batch_size','2000000')}}, 'worker_batch_size', {{var('worker_batch_size','100000')}}, 'batch_call_limit', {{var('batch_call_limit','1000')}}, 'call_type', 'RT'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_decoder']
) }}

{% if execute %}
    {% set min_event_block_id_query %}
        SELECT
            min(block_id)
        FROM
            {{ ref('silver__events') }}
        WHERE 
            block_timestamp >= CURRENT_DATE - 2
    {% endset %}
    {% set min_event_block_id = run_query(min_event_block_id_query).columns[0].values()[0] %}


    {% set idls_to_decode_query %}
        SELECT
            concat('\'', listagg(program_id, '\',\'') , '\'')
        FROM
            {{ ref('silver__verified_idls') }}
        WHERE   
            program_id <> 'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH'
            and is_active
    {% endset %}
    {% set idls_to_decode = run_query(idls_to_decode_query)[0][0] %}
{% endif %}

WITH event_subset AS (
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
        {{ ref('silver__events') }} AS e
    WHERE
        e.block_timestamp >= CURRENT_DATE - 2
        AND e.succeeded
        AND e.program_id IN ({{ idls_to_decode }})
    UNION ALL
    SELECT
        e.program_id AS inner_program_id,
        e.tx_id,
        e.instruction_index AS index,
        e.inner_index,
        e.instruction,
        e.block_id,
        e.block_timestamp,
        {{ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.instruction_index','inner_index','inner_program_id']) }} as id
    FROM
        {{ ref('silver__events_inner') }} AS e
    WHERE
        e.block_timestamp >= CURRENT_DATE - 2
        AND e.succeeded
        AND e.program_id IN ({{ idls_to_decode }})
        AND (
            (
                inner_program_id IN ('FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn','SNPRohhBurQwrpwAptw1QYtpFdfEKitr4WSJ125cN1g','GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY','JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4','DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M','PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu','LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo','PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY','6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
                AND array_size(e.instruction:accounts) > 1
            )
            OR inner_program_id NOT IN ('FLASH6Lo6h3iasJKWDs2F8TkW2UKf3s15C8PMGuVfgBn','SNPRohhBurQwrpwAptw1QYtpFdfEKitr4WSJ125cN1g','GovaE4iu227srtG2s3tZzB4RmWBzw8sTwrCLZz7kN7rY','JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4','DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M','PERPHjGBqRHArX4DySjwM6UJHiR3sWAatqfdBS2qQJu','LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo','PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY','6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P')
        )
),
completed_subset AS (
    SELECT
        block_id,
        complete_decoded_instructions_3_id as id
    FROM
        {{ ref('streamline__complete_decoded_instructions_3') }}
    WHERE
        block_id >= {{ min_event_block_id }} --ensure we at least prune to last 2 days worth of blocks since the dynamic below will scan everything
        AND block_id >= (
            SELECT
                min(block_id)
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
    event_subset AS e
LEFT OUTER JOIN 
    completed_subset AS c
    ON c.block_id = e.block_id
    AND e.id = c.id
WHERE
    c.block_id IS NULL