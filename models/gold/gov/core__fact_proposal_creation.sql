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
    *
FROM
    {{ ref('gov__fact_proposal_creation') }}