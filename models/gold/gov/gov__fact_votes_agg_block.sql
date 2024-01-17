{{ config(
  materialized = 'view',
  meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}},
  tags = ['scheduled_non_core']
) }}

SELECT 
    block_timestamp,
    block_id,
    num_votes,
    COALESCE (
        votes_agg_block_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id']
        ) }}
    ) AS fact_votes_agg_block_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__votes_agg_block') }}
