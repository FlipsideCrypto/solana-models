{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} },
    tags = ['scheduled_non_core','exclude_change_tracking']
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
    delegated_shares,
    COALESCE (
        gauges_votes_saber_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','voter','gauge']
        ) }}
    ) AS fact_gauges_votes_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
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
    delegated_shares,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'voter', 'voter_nft', 'gauge']
    ) }} AS fact_gauges_votes_id,
    '2000-01-01' AS inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__gauges_votes_marinade_view') }}
