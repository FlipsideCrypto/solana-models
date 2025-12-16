
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['stablecoins_daily_supply_id'],
    cluster_by = ['block_date','modified_timestamp::DATE'],
    tags = ['daily']
) }}

WITH 

{% if is_incremental() %}
max_ts AS (
    SELECT MAX(modified_timestamp) AS max_modified_timestamp
    FROM {{ this }}
),
incremental_dates AS (
    -- Get all distinct dates that have been updated in any source table
    SELECT DISTINCT block_date
    FROM {{ ref('silver__stablecoins_daily_supply_by_address') }}
    WHERE modified_timestamp > (SELECT max_modified_timestamp FROM max_ts)
    UNION
    SELECT DISTINCT block_timestamp::DATE AS block_date
    FROM {{ ref('silver__stablecoins_mint_burn') }}
    WHERE modified_timestamp > (SELECT max_modified_timestamp FROM max_ts)
    UNION
    SELECT DISTINCT block_timestamp::DATE AS block_date
    FROM {{ ref('silver__stablecoins_transfers') }}
    WHERE modified_timestamp > (SELECT max_modified_timestamp FROM max_ts)
),
{% endif %}

stablecoin_metadata AS (
    SELECT
        token_address,
        symbol,
        name,
        label,
        decimals
    FROM {{ ref('defi__dim_stablecoins') }}
    WHERE token_address IS NOT NULL
),

-- Aggregate daily balances by mint from the address-level data
daily_balances AS (
    SELECT
        block_date,
        mint,
        SUM(amount) AS total_supply,
        SUM(cex_balance) AS amount_in_cex,
        SUM(bridge_balance) AS amount_in_bridges,
        SUM(dex_balance) AS amount_in_dex_liquidity_pools,
        SUM(lending_pool_balance) AS amount_in_lending_pools,
        COUNT(DISTINCT owner) AS total_holders
    FROM {{ ref('silver__stablecoins_daily_supply_by_address') }}
    
{% if is_incremental() %}
WHERE
    block_date IN (
        SELECT
            block_date
        FROM
            incremental_dates
    )
{% endif %}
    
    GROUP BY block_date, mint
),

-- Aggregate daily mint/burn amounts
daily_mint_burn AS (
    SELECT
        block_timestamp::DATE AS block_date,
        mint,
        SUM(CASE WHEN event_name = 'Mint' THEN amount ELSE 0 END) AS amount_minted,
        SUM(CASE WHEN event_name = 'Burn' THEN amount ELSE 0 END) AS amount_burned
    FROM {{ ref('silver__stablecoins_mint_burn') }}
    
{% if is_incremental() %}
WHERE
    block_timestamp::DATE IN (
        SELECT
            block_date
        FROM
            incremental_dates
    )
{% endif %}
    
    GROUP BY block_timestamp::DATE, mint
),

-- Aggregate daily transfer amounts
daily_transfers AS (
    SELECT
        block_timestamp::DATE AS block_date,
        mint,
        SUM(amount) AS amount_transferred
    FROM {{ ref('silver__stablecoins_transfers') }}
    
{% if is_incremental() %}
WHERE
    block_timestamp::DATE IN (
        SELECT
            block_date
        FROM
            incremental_dates
    )
{% endif %}
    
    GROUP BY block_timestamp::DATE, mint
),

-- Combine all metrics
combined_metrics AS (
    SELECT
        db.block_date,
        db.mint,
        db.total_supply,
        db.amount_in_cex,
        db.amount_in_bridges,
        db.amount_in_dex_liquidity_pools,
        db.amount_in_lending_pools,
        db.total_holders,
        COALESCE(dmb.amount_minted, 0) AS amount_minted,
        COALESCE(dmb.amount_burned, 0) AS amount_burned,
        COALESCE(dt.amount_transferred, 0) AS amount_transferred
    FROM daily_balances db
    LEFT JOIN daily_mint_burn dmb
        ON db.block_date = dmb.block_date 
        AND db.mint = dmb.mint
    LEFT JOIN daily_transfers dt
        ON db.block_date = dt.block_date
        AND db.mint = dt.mint
)

SELECT
    cm.block_date,
    cm.mint as token_address,
    sm.symbol,
    sm.name,
    sm.label,
    sm.decimals,
    cm.total_supply,
    cm.total_holders,
    cm.amount_in_cex,
    cm.amount_in_bridges,
    cm.amount_in_dex_liquidity_pools,
    cm.amount_in_lending_pools,
    -- Calculate amount_in_contracts as sum of all categorized amounts
    (cm.amount_in_cex + cm.amount_in_bridges + cm.amount_in_dex_liquidity_pools + cm.amount_in_lending_pools) AS amount_in_contracts,
    cm.amount_minted,
    cm.amount_burned,
    cm.amount_transferred,
    {{ dbt_utils.generate_surrogate_key(['cm.block_date', 'cm.mint']) }} AS stablecoins_daily_supply_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM combined_metrics cm
INNER JOIN stablecoin_metadata sm
    ON cm.mint = sm.token_address
WHERE cm.block_date IS NOT NULL
    AND cm.mint IS NOT NULL
