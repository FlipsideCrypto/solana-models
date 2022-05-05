{{ config(
    materialized = 'view'
) }}

SELECT 
    'marinade' as program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    voter,
    voter_account,
    voter_nft,
    proposal
FROM
    {{ ref('silver__proposal_votes_marinade') }}