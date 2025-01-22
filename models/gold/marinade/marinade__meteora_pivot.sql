/* TODO: ephemeral names are working properly with our custom naming macro, has to be view for now 
but external user should not have perms to select from this view */

/* this only needs to be run once */
{{
    config(
        materialized = 'view',
        tags = ['exclude_change_tracking']
    )
}}

with base as (
select 
    lp.*,
    row_number() over (partition by lp.tx_id, lp.index, lp.action order by lp.inner_index) as rn
from 
    {{ ref('silver__liquidity_pool_actions_meteora') }} AS lp
inner join {{ ref('marinade__dim_pools') }} AS m 
    on lp.liquidity_pool_address = m.pool_address
where 
    lp.succeeded
    AND action in (
        'addBalanceLiquidity',
        'addImbalanceLiquidity',
        'bootstrapLiquidity',
        'removeBalanceLiquidity',
        'removeLiquiditySingleSide'
    )
),
pre_final as (
select 
    b1.* exclude(inner_index),
    iff(b1.inner_index=1,NULL,b1.inner_index-2) AS inner_index,
    b2.mint as b_mint,
    b2.amount AS b_amount
from base b1
left join
    base b2
    on b1.tx_id = b2.tx_id
    and b1.index = b2.index
    and b1.action = b2.action
    and b1.rn = 1
    and b2.rn <> 1
where
    b1.rn = 1
)
select
    block_id,
    block_timestamp,
    tx_id,
    index,
    inner_index,
    action AS event_type,
    liquidity_pool_address AS pool_address,
    liquidity_provider AS provider_address,
    mint AS token_a_mint,
    amount AS token_a_amount,
    b_mint AS token_b_mint,
    b_amount AS token_b_amount,
    program_id,
    modified_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_meteora_id
from pre_final
