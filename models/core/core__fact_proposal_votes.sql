{{ config(
    materialized = 'view'
) }}

SELECT 
    'tribeca' as governance_platform, 
    'marinade' as program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    voter_account,
    voter_nft,
    proposal, 
    'NULL' AS realms_id, 
    'NULL' AS vote_choice, 
    'NULL' AS vote_rank, 
    'NULL' AS vote_weight

FROM
    {{ ref('silver__proposal_votes_marinade') }}

UNION ALL 

SELECT 
    'realms' as governance_platform, 
    program_id as program_name, 
    block_timestamp, 
    block_id, 
    tx_id, 
    succeeded,  
    voter, 
    vote_account, 
    'NULL' AS voter_nft, 
    proposal,
    realms_id, 
    vote_choice, 
    vote_rank, 
    vote_weight

FROM
    {{ ref('silver__proposal_votes_realms') }}
