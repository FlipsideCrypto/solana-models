
{{ config(
    materialized = 'incremental',
    unique_key = ['stablecoins_daily_supply_by_address_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_date','modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}


WITH verified_stablecoins AS (
    SELECT
        token_address,
        decimals,
        symbol,
        NAME
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        is_verified
        and token_address IS NOT NULL
),

balance_base AS (
    SELECT
        balance_date as block_date,
        account,
        mint,
        amount,
        owner
    FROM {{ ref('core__fact_token_daily_balances') }} a
    INNER JOIN verified_stablecoins b 
        ON a.mint = b.token_address
    WHERE block_date = '2025-12-09'
    -- {% if is_incremental() %}
    -- WHERE block_date >= (
    --     SELECT
    --         MAX(block_date)
    --     FROM
    --         {{ this }}
    -- )
    -- {% endif %}
),

lp_token_accounts AS (
    SELECT DISTINCT token_a_account AS token_account
    FROM {{ ref('silver__liquidity_pools') }} a
    INNER JOIN verified_stablecoins b
        ON a.token_a_mint = b.token_address
    
    UNION
    
    SELECT DISTINCT token_b_account AS token_account 
    FROM {{ ref('silver__liquidity_pools') }} a
    INNER JOIN verified_stablecoins b
        ON a.token_b_mint = b.token_address
),

bridge_vaults_accounts AS (
    SELECT vault_token_address AS token_account
    FROM {{ ref('silver__stablecoin_bridge_vault_seed') }}
),

lending_pool_accounts AS (
    SELECT DISTINCT bank_liquidity_vault AS token_account
    FROM 
    {{ ref('silver__lending_marginfi_deposits') }} a
    INNER JOIN verified_stablecoins b
        ON a.token_address = b.token_address
    where a.block_timestamp::date = '2025-12-09'
        
    
    UNION ALL
    
    SELECT DISTINCT liquidity_supply_vault AS token_account
    FROM {{ ref('silver__lending_kamino_deposits') }} a
    INNER JOIN verified_stablecoins b
        ON a.token_address = b.token_address
    where a.block_timestamp::date = '2025-12-09'
),
cex_list AS (
    SELECT
        DISTINCT address,
        'cex' AS contract_type
    FROM
        solana.core.dim_labels
    WHERE
        label_type = 'cex'
)

SELECT 
    block_date,
    account,
    mint,
    amount,
    owner,
    CASE 
        WHEN b.token_account IS NOT NULL THEN amount 
        ELSE 0 
    END AS dex_balance,
    CASE 
        WHEN c.token_account IS NOT NULL THEN amount 
        ELSE 0 
    END AS bridge_balance,
    CASE 
        WHEN d.token_account IS NOT NULL THEN amount 
        ELSE 0 
    END AS lending_pool_balance,
    CASE 
        WHEN e.address IS NOT NULL THEN amount 
        ELSE 0 
    END AS cex_balance,
    {{ dbt_utils.generate_surrogate_key(['block_date','account','mint']) }} AS stablecoins_daily_supply_by_address_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM balance_base a
LEFT JOIN lp_token_accounts b
    ON a.account = b.token_account
LEFT JOIN bridge_vaults_accounts c
    ON a.account = c.token_account
LEFT JOIN lending_pool_accounts d
    ON a.account = d.token_account
left join cex_list e
on a.owner = e.address