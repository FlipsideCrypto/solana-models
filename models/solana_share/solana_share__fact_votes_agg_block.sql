{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

  SELECT 
    block_timestamp,
    block_id,
    num_votes
FROM {{ref('core__fact_votes_agg_block')}}
where block_timestamp::date between '2021-12-01' and '2021-12-31'