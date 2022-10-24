{{ 
    config(
      materialized='view',
      meta={
        'database_tags':{
            'table': {
                'PURPOSE': 'PRICES'
            }
        }
      }
    ) 
}}

SELECT 
    'coinmarketcap' as provider,
    recorded_hour,
    id,
    symbol, 
    close,
    imputed
FROM
    {{ ref('silver__token_prices_hourly') }}