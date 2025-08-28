{{ config(
    materialized = 'incremental',
    unique_key = ['ez_lending_liquidations_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, event_type, liquidator, debt_token, collateral_token)'),
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'LENDING' }}},
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% if is_incremental() %}
    {% set max_modified_query %}
    SELECT
        MAX(modified_timestamp) AS modified_timestamp
    FROM
        {{ this }}
    {% endset %}
    {% set max_modified_timestamp = run_query(max_modified_query)[0][0] %}
    {% endif %}
{% endif %}

WITH liquidations AS (
    SELECT 
        'kamino' AS platform,
        'kamino' AS protocol,
        'v1' AS version,
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        program_id,
        event_type,
        liquidator,
        borrower,
        collateral_token,
        debt_token,
        protocol_market,
        amount_raw,
        amount,
        lending_kamino_liquidations_id AS ez_lending_liquidations_id
    FROM {{ ref('silver__lending_kamino_liquidations') }}
    {% if is_incremental() %}
    WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
    {% endif %}
    UNION ALL
    SELECT 
        'marginfi v2' AS platform,
        'marginfi' AS protocol,
        'v2' AS version,
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        program_id,
        event_type,
        liquidator,
        borrower,
        collateral_token,
        debt_token,
        NULL AS protocol_market,
        amount_raw,
        amount,
        lending_marginfi_liquidations_id AS ez_lending_liquidations_id
    FROM {{ ref('silver__lending_marginfi_liquidations') }}
    {% if is_incremental() %}
    WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
    {% endif %}
),

prices AS (
    SELECT
        HOUR,
        token_address,
        symbol,
        price,
        is_verified
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        hour >= (
            SELECT
                MIN(DATE_TRUNC('hour', block_timestamp))
            FROM
                liquidations
        )
)

SELECT 
    a.platform,
    a.protocol,
    a.version,
    a.block_timestamp,
    a.block_id,
    a.tx_id,
    a.index,
    a.inner_index,
    a.program_id,
    a.event_type,
    a.liquidator,
    a.borrower,
    a.collateral_token,
    c.symbol AS collateral_token_symbol,
    c.is_verified AS collateral_token_is_verified,
    a.debt_token,
    b.symbol AS debt_token_symbol,
    b.is_verified AS debt_token_is_verified,
    a.protocol_market,
    a.amount_raw,
    a.amount,
    ROUND(
        a.amount * b.price,
        2
    ) AS amount_usd,
    a.ez_lending_liquidations_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM liquidations a 
LEFT JOIN prices b
    ON a.debt_token = b.token_address
    AND DATE_TRUNC('hour', a.block_timestamp) = b.hour
LEFT JOIN prices c
    ON a.collateral_token = c.token_address
    AND DATE_TRUNC('hour', a.block_timestamp) = c.hour
