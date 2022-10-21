{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'GOVERNANCE'
            }
        }
    }
) }}

SELECT 
    'marinade' as program_name,
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    signer,
    gauge,
    gaugemeister,
    validator_account
FROM
    {{ ref('silver__gauges_creates_marinade') }}