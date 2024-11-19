{{ config (
    materialized = "table",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_instructions_decoder_v2(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'decoded_instructions_3', 'sql_limit', {{var('sql_limit','5000000')}}, 'producer_batch_size', {{var('producer_batch_size','2000000')}}, 'worker_batch_size', {{var('worker_batch_size','100000')}}, 'batch_call_limit', {{var('batch_call_limit','1000')}}, 'call_type', 'RT'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline_decoder']
) }}

/* 
while we are running in parallel, can just select from the existing table 
once we are done, we can move the existing code into this table 
and it should be mostly the same except for the completed table references
*/
SELECT
    *
FROM
    {{ ref('streamline__decode_instructions_2_realtime') }}
