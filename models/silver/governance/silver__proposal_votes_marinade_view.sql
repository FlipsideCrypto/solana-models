{{ config(
  materialized = 'view'
) }}

SELECT 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    voter_nft,
    voter_account,
    proposal,
    _inserted_timestamp,
    proposal_votes_marinade_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
  {{ source(
    'solana_silver',
    'proposal_votes_marinade'
  ) }}