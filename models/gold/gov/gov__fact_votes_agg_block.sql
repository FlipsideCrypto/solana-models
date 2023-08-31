{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}}
) }}

SELECT 
    block_timestamp,
    block_id,
    num_votes
FROM
    {{ ref('silver__votes_agg_block') }}
