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
    gauge,
    NULL AS power,
    delegated_shares
FROM
 {{ source(
    'solana_silver',
    'gauges_votes_marinade'
  ) }}