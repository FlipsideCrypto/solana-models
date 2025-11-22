{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS' }}},
    tags = ['scheduled_non_core', 'exclude_change_tracking']
) }}

SELECT
    day_,
    protocol,
    n_users,
    n_quality_users,
    n_transactions,
    n_quality_transactions,
    usd_inflows,
    usd_outflows,
    net_usd_inflow,
    gross_usd_volume,
    quality_usd_inflows,
    quality_usd_outflows,
    quality_net_usd,
    quality_gross_usd
FROM
    {{ source(
        'crosschain_chain_stats',
        'ez_solana_protocol_metrics'
    ) }}
