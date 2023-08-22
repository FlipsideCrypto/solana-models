{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}}
) }}

SELECT
    'saber' AS program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    NULL AS voter_nft,
    gauge,
    power,
    delegated_shares
FROM
    {{ ref('silver__gauges_votes_saber') }}
UNION
SELECT
    'marinade' AS program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    voter_nft,
    gauge,
    NULL AS power,
    delegated_shares
FROM
    {{ ref('silver__gauges_votes_marinade') }}
