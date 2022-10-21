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
    'realms' as governance_platform, 
    program_id as program_name, 
    block_timestamp, 
    block_id, 
    tx_id, 
    succeeded,  
    realms_id, 
    proposal, 
    proposal_writer, 
    proposal_name, 
    vote_type, 
    vote_options
FROM 
    {{ ref('silver__proposal_creation_realms') }}