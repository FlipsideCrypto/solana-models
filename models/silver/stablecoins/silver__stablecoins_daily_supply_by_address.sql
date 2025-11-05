
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

lp_token_addresses AS (
    SELECT DISTINCT token_a_account AS token_address 
    FROM {{ ref('silver__liquidity_pools') }} a
    INNER JOIN verified_stablecoins b
        ON a.token_a_mint = b.token_address
    
    UNION ALL
    
    SELECT DISTINCT token_b_account AS token_address 
    FROM {{ ref('silver__liquidity_pools') }} a
    INNER JOIN verified_stablecoins b
        ON a.token_b_mint = b.token_address
),

bridge_vaults AS (
    SELECT vault_token_address AS token_address
    FROM {{ ref('silver__stablecoin_bridge_vault_seed') }}
),

balance_base AS (
    SELECT
        balance_date,
        account,
        mint,
        amount,
        owner
    FROM {{ ref('core__fact_token_daily_balances') }} a
    INNER JOIN verified_stablecoins b 
        ON a.mint = b.token_address
    WHERE balance_date = '2025-10-29'
    -- {% if is_incremental() %}
    -- WHERE balance_date >= (
    --     SELECT
    --         MAX(balance_date)
    --     FROM
    --         {{ this }}
    -- )
    -- {% endif %}
)

SELECT 
    balance_date,
    account,
    mint,
    amount,
    owner,
    CASE 
        WHEN b.token_address IS NOT NULL THEN amount 
        ELSE 0 
    END AS dex_balance,
    CASE 
        WHEN c.token_address IS NOT NULL THEN amount 
        ELSE 0 
    END AS bridge_balance,
    {{ dbt_utils.generate_surrogate_key(['balance_date','account','mint']) }} AS stablecoins_daily_supply_by_address_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM balance_base a
LEFT JOIN lp_token_addresses b
    ON a.account = b.token_address
LEFT JOIN bridge_vaults c
    ON a.account = c.token_address