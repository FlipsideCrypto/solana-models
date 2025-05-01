{{ config(
    materialized = 'table',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}}
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
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    {{ ref('silver__gauges_creates_marinade_view') }}
