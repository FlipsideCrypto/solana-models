{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

  SELECT 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    event_type,
    program_id,
    instruction,
    inner_instruction
FROM {{ref('core__fact_events')}}
where block_timestamp::date between '2021-12-01' and '2021-12-31'