{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_instructions_decoder(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'decoded_instructions_2', 'sql_limit', {{var('sql_limit','10000000')}}, 'producer_batch_size', {{var('producer_batch_size','10000000')}}, 'worker_batch_size', {{var('worker_batch_size','100000')}}, 'batch_call_limit', {{var('batch_call_limit','1000')}}, 'call_type', 'RT_PYTH'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_decoder']
) }}

{% if execute %}
    {% set max_block_id_query %}
        select max(block_id)
        from {{ ref('silver__events') }}
    {% endset %}
    {% set max_block_id = run_query(max_block_id_query).columns[0].values()[0] %}
    {% set min_block_id = max_block_id - 150000 %}
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
        {{ ref('silver__events') }}
        e
    WHERE
        e.program_id = 'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH'
    AND 
        e.block_timestamp >= CURRENT_DATE - 1
    AND 
        e.block_id between {{ min_block_id }} and {{ max_block_id}}
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
        JOIN table(flatten(e.inner_instruction:instructions)) i 
    WHERE
        ARRAY_CONTAINS('FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH'::variant, e.inner_instruction_program_ids) 
    AND
        e.block_timestamp >= CURRENT_DATE - 1
    AND 
        e.block_id between {{ min_block_id }} and {{ max_block_id}}
    AND 
        e.succeeded
    AND 
        inner_program_id = 'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH'
    
),
completed_subset AS (
    SELECT
        block_id,
        complete_decoded_instructions_2_id as id
    FROM
        {{ ref('streamline__complete_decoded_instructions_2') }}
    WHERE
        block_id >= (
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