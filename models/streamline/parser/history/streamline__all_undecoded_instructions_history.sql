{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_program_parser(object_construct('realtime', 'False'))",
        target = "{{this.schema}}.{{this.identifier}}"
    ),
    tags = ['streamline'],
) }}

with m as (
    select max(block_id) as max_block_id
    from {{ ref('streamline__all_undecoded_instructions_history_queue') }} h
)
select 
    INDEX,
    program_id,
    instruction,
    tx_id,
    h.block_id
from {{ ref('streamline__all_undecoded_instructions_history_queue') }} h
where h.block_id between (select max_block_id-200000 from m) and (select max_block_id from m)