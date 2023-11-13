{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'GOVERNANCE' }}},
    tags = ['scheduled_non_core']
) }}

SELECT
    'realms' AS governance_platform,
    program_id AS program_name,
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
