
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['stablecoins_daily_supply_by_address_id'],
    cluster_by = ['block_date','modified_timestamp::DATE'],
    tags = ['daily']
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
        token_address IS NOT NULL
),

{% if is_incremental() %}
newly_verified_stablecoins AS (
    SELECT
        token_address,
        decimals,
        symbol,
        NAME
    FROM
        {{ ref('defi__dim_stablecoins') }}
    WHERE
        IFNULL(
            is_verified_modified_timestamp,
            '1970-01-01' :: TIMESTAMP
        ) > DATEADD(
            'day',
            -8,
            (
                SELECT
                    MAX(modified_timestamp) :: DATE
                FROM
                    {{ this }}
            )
        )
),

newly_verified_balances AS (
    SELECT
        balance_date as block_date,
        account,
        mint,
        amount,
        owner
    FROM {{ ref('core__fact_token_daily_balances') }} a
    INNER JOIN newly_verified_stablecoins b 
        ON a.mint = b.token_address
    WHERE balance_date >= '2025-06-01'

),
{% endif %}

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
    WHERE balance_date >= '2025-06-01'
{% if is_incremental() %}
and a.modified_timestamp > (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}
),

all_balances AS (
    SELECT * FROM balance_base
    
    {% if is_incremental() %}
    UNION ALL
    SELECT * FROM newly_verified_balances
    {% endif %}
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
    where a.block_timestamp::date >= '2025-06-01'
    
    UNION ALL
    
    SELECT DISTINCT liquidity_supply_vault AS token_account
    FROM {{ ref('silver__lending_kamino_deposits') }} a
    INNER JOIN verified_stablecoins b
        ON a.token_address = b.token_address
    where a.block_timestamp::date >= '2025-06-01'
),
cex_list AS (
    SELECT
        DISTINCT address,
        'cex' AS contract_type
    FROM
        {{ ref('core__dim_labels') }}
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
FROM all_balances a
LEFT JOIN lp_token_accounts b
    ON a.account = b.token_account
LEFT JOIN bridge_vaults_accounts c
    ON a.account = c.token_account
LEFT JOIN lending_pool_accounts d
    ON a.account = d.token_account
left join cex_list e
    on a.owner = e.address