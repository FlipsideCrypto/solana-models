-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['stablecoins_daily_supply_by_mint_id'],
    incremental_predicates = ["dynamic_range_predicate", "balance_date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['balance_date','modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

-- TODO: add lending pool addresses. Can just use deposits, but currently those tables dont have a way to relate to balances
-- need to pull out the token address for the lending deposits.


WITH verified_stablecoins AS (

    SELECT
        token_address,
        decimals,
        symbol,
        NAME
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        -- is_verified
        -- AND 
        token_address IS NOT NULL
),
lp_token_addresses as (
select distinct(token_a_account) as token_address from solana.silver.liquidity_pools a
inner join verified_stablecoins b
on a.token_a_mint = b.token_address
where pool_address = '2EXiumdi14E9b8Fy62QcA5Uh6WdHS2b38wtSxp72Mibj'
union all
select distinct(token_b_account) as token_address from solana.silver.liquidity_pools a
inner join verified_stablecoins b
on a.token_b_mint = b.token_address
where pool_address = '2EXiumdi14E9b8Fy62QcA5Uh6WdHS2b38wtSxp72Mibj'
)
-- select * from lp_pool_token_addreses
,
bridge_vaults as (
        SELECT vault_token_address as token_address
    FROM solana_dev.silver.stablecoin_bridge_vault_seed
    )
,
balance_base as (
    SELECT
        balance_date,
        account,
        mint,
        amount,
        owner
    FROM {{ ref('core__fact_token_daily_balances') }} a
        INNER JOIN verified_stablecoins b on a.mint = b.token_address
    where balance_date = '2025-10-29'
    -- {% if is_incremental() %}
    -- where balance_date >= (
    --     SELECT
    --         MAX(balance_date)
    --     FROM
    --         {{ this }}
    -- )
    -- {% endif %}

)
select 
        balance_date,
        account,
        mint,
        amount,
        owner,
    CASE WHEN b.token_address IS NOT NULL THEN amount ELSE 0 END AS dex_balance,
    CASE WHEN c.token_address IS NOT NULL THEN amount ELSE 0 END AS bridge_balance,
        {{ dbt_utils.generate_surrogate_key(['balance_date','account','mint']) }} AS stablecoins_daily_supply_by_address_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    from balance_base a
    left join lp_token_addresses b
    on a.account = b.token_address
    left join bridge_vaults c
    on a.account = c.token_address


