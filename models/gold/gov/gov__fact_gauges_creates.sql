{{ config(
    materialized = 'view',
    post_hook = 'ALTER VIEW {{this}} SET CHANGE_TRACKING = TRUE;',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }} },
    tags = ['scheduled_non_core']
) }}

SELECT
    'marinade' AS program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    signer,
    gauge,
    gaugemeister,
    validator_account,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS fact_gauges_creates_id,
    '2000-01-01' AS inserted_timestamp,
    '2000-01-01' AS modified_timestamp
FROM
    {{ ref('silver__gauges_creates_marinade_view') }}
