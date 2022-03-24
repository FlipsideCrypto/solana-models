{{ config(
    materialized = 'view'
) }}

SELECT 
    'saber' as program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    gauge,
    power,
    delegated_shares
FROM
    {{ ref('silver__saber_gauges_votes') }}
