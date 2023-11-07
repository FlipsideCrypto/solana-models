{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    cluster_by = ['block_timestamp::date'],
    tags = ['share']
) }}

  SELECT 
    governance_platform, 
    program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    voter_account,
    voter_nft,
    proposal, 
    realms_id, 
    vote_choice, 
    vote_rank, 
    vote_weight
FROM {{ref('gov__fact_proposal_votes')}}
where block_timestamp::date between '2021-12-01' and '2021-12-31'