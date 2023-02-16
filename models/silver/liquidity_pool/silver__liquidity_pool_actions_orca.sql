{{ config(
    materialized = 'view'
) }}

with base as (
    select 
        *
    from 
        {{ ref('silver__burns_orca_non_whirlpool') }}
    union 
    select 
        *
    from 
        {{ ref('silver__burns_orca_whirlpool') }}
    union
        select 
        *
    from 
        {{ ref('silver__mints_orca_non_whirlpool') }}
    union 
        select 
        *
    from 
        {{ ref('silver__mints_orca_whirlpool') }}
    union 
        select 
        *
    from 
        {{ ref('silver__pool_transfers_orca_non_whirlpool') }}
    union 
        select 
        *
    from 
        {{ ref('silver__pool_transfers_orca_whirlpool') }}
)
select 
    *
from base 