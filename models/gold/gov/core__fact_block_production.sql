{{ config(
    materialized = 'view',
    meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'VALIDATOR'
            }
        }
    }
) }}

SELECT
    *
FROM
    {{ ref('gov__fact_block_production') }}