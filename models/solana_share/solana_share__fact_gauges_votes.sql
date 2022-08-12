{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

  SELECT 
    program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    voter_nft,
    gauge,
    power,
    delegated_shares
FROM {{ref('core__fact_gauges_votes')}}
where block_timestamp::date between '2021-12-01' and '2021-12-31'