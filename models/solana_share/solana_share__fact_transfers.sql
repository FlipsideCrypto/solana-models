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
    index,
    tx_from,
    tx_to,
    amount,
    mint
FROM {{ref('core__fact_transfers')}}
where block_timestamp::date between '2021-12-01' and '2021-12-31'