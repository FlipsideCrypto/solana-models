{{ config(
    materialized = 'view',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }} },
    tags = ['scheduled_non_core_hourly','exclude_change_tracking']
) }}

WITH swaps AS (

    SELECT
        swap_program,
        block_id,
        block_timestamp,
        tx_id,
        program_id,
        swapper,
        swap_from_mint,
        swap_from_amount,
        swap_to_mint,
        swap_to_amount,
        _log_id,
        fact_swaps_id AS ez_swaps_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('defi__fact_swaps') }}
    WHERE
        succeeded
),
prices AS (
    SELECT
        HOUR,
        token_address,
        symbol,
        price
    FROM
        {{ ref('price__ez_prices_hourly') }}
)
SELECT
    d.swap_program,
    d.block_id,
    d.block_timestamp,
    d.tx_id,
    d.program_id,
    d.swapper,
    d.swap_from_mint,
    p_in.symbol AS swap_from_symbol,
    d.swap_from_amount,
    ROUND(
        p_in.price * d.swap_from_amount,
        2
    ) AS swap_from_amount_usd,
    d.swap_to_mint,
    p_out.symbol AS swap_to_symbol,
    d.swap_to_amount,
    ROUND(
        p_out.price * d.swap_to_amount,
        2
    ) AS swap_to_amount_usd,
    d._log_id,
    d.inserted_timestamp,
    d.modified_timestamp,
    d.ez_swaps_id,
FROM
    swaps d
    LEFT JOIN prices p_in
    ON d.swap_from_mint = p_in.token_address
    AND DATE_TRUNC(
        'hour',
        d.block_timestamp
    ) = p_in.hour
    LEFT JOIN prices p_out
    ON d.swap_to_mint = p_out.token_address
    AND DATE_TRUNC(
        'hour',
        d.block_timestamp
    ) = p_out.hour
