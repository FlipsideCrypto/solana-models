{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}},
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
    validator_account
FROM
    {{ ref('silver__gauges_creates_marinade_view') }}
