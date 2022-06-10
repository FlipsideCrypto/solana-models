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
    NULL as voter_nft,
    gauge,
    power,
    delegated_shares
FROM
    {{ ref('silver__gauges_votes_saber') }}
UNION 
SELECT 
    'marinade' as program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    voter_nft,
    gauge,
    NULL as power,
    delegated_shares
FROM
    {{ ref('silver__gauges_votes_marinade') }}
