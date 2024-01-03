{{ config(
    materialized = 'view',
    tags = ['scheduled_non_core']
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
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id', 'index', 'inner_index']
    ) }} AS liquidity_pool_actions_orca_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from base 