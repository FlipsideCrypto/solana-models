{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_instructions_decoder(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'decoded_instructions_2', 'sql_limit', {{var('sql_limit','100000000')}}, 'producer_batch_size', {{var('producer_batch_size','20000000')}}, 'worker_batch_size', {{var('worker_batch_size','500000')}}, 'batch_call_limit', {{var('batch_call_limit','1000')}}, 'call_type', 'backfill'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_decoder']
) }}

{% set backfill_date = '2023-03-04' %}


with completed_subset AS (
            SELECT
                block_id,
                program_id,
                complete_decoded_instructions_2_id as id
            FROM
                solana.streamline.complete_decoded_instructions_2
            WHERE
                program_id = 'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH'
            AND
                block_id between 0 and 1
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
        
    
md5(cast(coalesce(cast(e.block_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.tx_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.index as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(inner_index as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.program_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id
    FROM
        SOLANA.silver.events e
    WHERE
        e.program_id = 'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH'
    AND
        e.block_timestamp::date = '{{ backfill_date }}'
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
        
    
md5(cast(coalesce(cast(e.block_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.tx_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(e.index as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(inner_index as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(inner_program_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id
    FROM
        SOLANA.silver.events e
        JOIN table(flatten(e.inner_instruction:instructions)) i 
    WHERE
        ARRAY_CONTAINS('FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH'::variant, e.inner_instruction_program_ids) 
    AND
        e.block_timestamp::date = '{{ backfill_date }}'
    AND 
        e.succeeded
    AND 
        i.value :programId :: STRING = 'FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH'
    
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