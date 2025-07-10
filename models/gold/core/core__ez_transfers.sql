{{ config(
    materialized = 'view',
    tags = ['scheduled_core', 'exclude_change_tracking']
) }}

SELECT 
    a.block_timestamp,
    a.block_id,
    a.tx_id,
    COALESCE(SPLIT_PART(a.INDEX :: text, '.', 1) :: INT, a.INDEX :: INT) AS index,
    NULLIF(SPLIT_PART(a.INDEX :: text, '.', 2), '') :: INT AS inner_index,
    a.tx_from,
    a.tx_to,
    a.amount,
    a.amount * p.price AS amount_usd,
    a.mint,
    COALESCE(
        p.symbol,
        b.symbol
    ) AS symbol,
    COALESCE(
        p.is_verified,
        FALSE
    ) AS token_is_verified,
    a.fact_transfers_id AS ez_transfers_id,
    a.inserted_timestamp,
    a.modified_timestamp
FROM 
    {{ ref('core__fact_transfers') }} a
    LEFT JOIN {{ ref('price__ez_asset_metadata') }} b
    ON a.mint = b.token_address
    LEFT JOIN {{ ref('price__ez_prices_hourly') }} p
    ON a.mint = p.token_address
    AND DATE_TRUNC(
        'hour',
        a.block_timestamp
    ) = p.hour
