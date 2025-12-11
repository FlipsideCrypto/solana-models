{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    tags = ['daily']
) }}

SELECT
    block_date,
    token_address,
    symbol,
    NAME,
    label,
    decimals,
    total_supply,
    total_holders,
    amount_minted,
    amount_burned,
    amount_transferred,
    amount_in_cex,
    amount_in_bridges,
    amount_in_dex_liquidity_pools,
    amount_in_lending_pools,
    amount_in_contracts,
    inserted_timestamp,
    modified_timestamp,
    stablecoins_daily_supply_id AS ez_stablecoins_supply_id
FROM
    {{ ref('silver__stablecoins_daily_supply') }}