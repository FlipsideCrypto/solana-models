{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
) }}

with base as (
    select 
        *
    from 
        {{ ref('silver__burns_raydium') }}
    union
        select 
        *
    from 
        {{ ref('silver__mints_raydium') }}
    union 
        select 
        *
    from 
        {{ ref('silver__pool_transfers_raydium') }}
)
select 
    *
from base 