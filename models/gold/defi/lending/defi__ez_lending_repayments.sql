{{ config(
    materialized = 'incremental',
    unique_key = ['ez_lending_repayments_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, event_type, payer, token_address)'),
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

WITH repayments AS (
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
        payer,
        borrower,
        protocol_market,
        token_address,
        amount_raw,
        amount,
        lending_kamino_repays_id AS ez_lending_repayments_id
    FROM {{ ref('silver__lending_kamino_repays') }}
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
        payer,
        borrower,
        NULL AS protocol_market,
        token_address,
        amount_raw,
        amount,
        lending_marginfi_repays_id AS ez_lending_repayments_id
    FROM {{ ref('silver__lending_marginfi_repays') }}
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
                repayments
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
    a.payer,
    a.borrower,
    a.protocol_market,
    a.token_address,
    b.symbol AS token_symbol,
    a.amount_raw,
    a.amount,
    ROUND(
        a.amount * b.price,
        2
    ) AS amount_usd,
    a.ez_lending_repayments_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM repayments a
LEFT JOIN prices b
    ON a.token_address = b.token_address
    AND DATE_TRUNC('hour', a.block_timestamp) = b.hour
