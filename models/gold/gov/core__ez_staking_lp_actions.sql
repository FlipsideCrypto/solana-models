{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'STAKING'
            }
        }
    }
) }}

SELECT
    *
FROM
    {{ ref('gov__ez_staking_lp_actions') }}
   
