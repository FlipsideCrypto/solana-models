{{ config(
    materialized = 'view',
    post_hook = 'ALTER VIEW {{this}} SET CHANGE_TRACKING = TRUE;',
    tags = ['scheduled_core']
) }}

SELECT
    block_id,
    block_timestamp,
    network,
    chain_id,
    block_height,
    block_hash,
    previous_block_id,
    previous_block_hash,
    COALESCE (
        blocks_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id']
        ) }}
    ) AS fact_blocks_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__blocks') }}
