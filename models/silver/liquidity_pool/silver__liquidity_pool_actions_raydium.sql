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
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id', 'tx_id','index', 'inner_index']
    ) }} AS liquidity_pool_actions_raydium_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
from base 