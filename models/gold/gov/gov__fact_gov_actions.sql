{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}},
    tags = ['scheduled_non_core']
) }}

SELECT 
    'saber' as program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    signer,
    locker_account,
    null as locker_nft,
    mint,
    action,
    amount,
    COALESCE (
        gov_actions_saber_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_gov_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__gov_actions_saber') }}
UNION ALL
SELECT 
    'marinade' as program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    signer,
    locker_account,
    locker_nft,
    mint,
    action,
    amount,
    COALESCE (
        gov_actions_marinade_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id']
        ) }}
    ) AS fact_gov_actions_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__gov_actions_marinade') }}