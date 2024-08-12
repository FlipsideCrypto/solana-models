{{ config(
    materialized = 'view',
    post_hook = 'ALTER VIEW {{this}} SET CHANGE_TRACKING = TRUE;',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'Token' }} },
    tags = ['scheduled_non_core']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    INDEX,
    inner_index,
    event_type,
    mint,
    burn_amount,
    burn_authority,
    token_account,
    signers,
    DECIMAL,
    mint_standard_type,
    COALESCE (
        token_burn_actions_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id', 'index', 'inner_index', 'mint']
        ) }}
    ) AS fact_token_burn_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__token_burn_actions') }}
