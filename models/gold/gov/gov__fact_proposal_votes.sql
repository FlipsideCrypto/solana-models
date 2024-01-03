{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}},
    tags = ['scheduled_non_core']
) }}

SELECT
    'tribeca' AS governance_platform,
    'marinade' AS program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    voter_account,
    voter_nft,
    proposal,
    NULL AS realms_id,
    NULL AS vote_choice,
    NULL AS vote_rank,
    NULL AS vote_weight,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'voter_nft', 'proposal']
    ) }} AS fact_proposal_votes_id,
    '2000-01-01' as inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__proposal_votes_marinade_view') }}
UNION ALL
SELECT
    'realms' AS governance_platform,
    program_id AS program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    vote_account,
    NULL AS voter_nft,
    proposal,
    realms_id,
    vote_choice,
    vote_rank,
    vote_weight,
  COALESCE (
    proposal_votes_realms_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','index']
        ) }}
    ) AS fact_proposal_votes_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__proposal_votes_realms') }}
