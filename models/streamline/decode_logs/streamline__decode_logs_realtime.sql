{{ config (
    materialized = "table",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_instructions_decoder(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'decoded_logs', 'sql_limit', {{var('sql_limit','5000000')}}, 'producer_batch_size', {{var('producer_batch_size','2000000')}}, 'worker_batch_size', {{var('worker_batch_size','100000')}}, 'batch_call_limit', {{var('batch_call_limit','1000')}}, 'call_type', 'logs'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_decoder_logs']
) }}

{% if execute %}
    {% set min_event_block_id_query %}
        SELECT
            min(block_id)
        FROM 
            {{ ref('silver__blocks') }}
        WHERE 
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
        i.value :programId :: STRING AS inner_program_id,
        e.tx_id,
        e.index,
        i.index AS inner_index,
        NULL AS log_index,
        i.value AS instruction,
        e.block_id,
        e.block_timestamp,
        {{ dbt_utils.generate_surrogate_key(['e.block_id','e.tx_id','e.index','inner_index','log_index','inner_program_id']) }} as id
    FROM
        {{ ref('silver__events') }} e
    JOIN
        table(flatten(e.inner_instruction:instructions)) i 
    JOIN
        idl_in_play b
        ON b.program_id = i.value :programId :: STRING
    WHERE
        e.block_timestamp >= CURRENT_DATE - 2   
        AND e.succeeded
        AND e.program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
        AND inner_program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
        AND array_size(i.value:accounts::array) = 1
    UNION ALL
    SELECT 
        program_id,
        tx_id,
        index,
        inner_index,
        log_index,
        object_construct('accounts',[],'data',data,'programId',program_id) as instruction,
        block_id,
        block_timestamp,
        {{ dbt_utils.generate_surrogate_key(['block_id','tx_id','index','inner_index','log_index','program_id']) }} as id
    FROM
        {{ ref('silver__transaction_logs_program_data') }} 
    WHERE 
        block_timestamp >= CURRENT_DATE - 2
        AND program_id = 'TSWAPaqyCSx2KABk68Shruf4rp7CxcNi8hAsbdwmHbN'
),
completed_subset AS (
    SELECT
        block_id,
        complete_decoded_logs_id as id
    FROM
        {{ ref('streamline__complete_decoded_logs') }}
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
    e.inner_program_id as program_id,
    e.tx_id,
    e.index,
    e.inner_index,
    e.log_index,
    e.instruction,
    e.block_id,
    e.block_timestamp
FROM
    event_subset e
LEFT OUTER JOIN 
    completed_subset C
    ON C.block_id = e.block_id
    AND e.id = C.id
WHERE
    C.block_id IS NULL
