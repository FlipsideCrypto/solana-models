{{ config(
    materialized = 'incremental',
    unique_key = ['ez_lending_deposits_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, event_type, depositor, token_address)'),
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

WITH deposits AS (
select 
    'kamino' as platform,
    'kamino' as protocol,
    'v1' as version,
    block_timestamp,
    block_id,
    tx_id,
    index,
    inner_index,
    program_id,
    event_type,
    depositor,
    protocol_market,
    token_address,
    amount_raw,
    amount,
    lending_kamino_deposits_id as ez_lending_deposits_id
    from {{ ref('silver__lending_kamino_deposits') }}
    {% if is_incremental() %}
AND
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
    UNION ALL
SELECT 
    'marginfi v2' as platform,
    'marginfi' as protocol,
    'v2' as version,
    block_timestamp,
    block_id,
    tx_id,
    index,
    inner_index,
    program_id,
    event_type,
    depositor,
    NULL as protocol_market,
    token_address,
    amount_raw,
    amount,
    lending_marginfi_deposits_id as ez_lending_deposits_id
FROM
    {{ ref('silver__lending_marginfi_deposits') }} a
    {% if is_incremental() %}
AND
    modified_timestamp >= '{{ max_modified_timestamp }}'
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
                deposits
        )

)
select 
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
    a.depositor,
    a.protocol_market,
    a.token_address,
    b.symbol as token_symbol,
    a.amount_raw,
    a.amount,
        ROUND(
      a.amount * b.price,
      2
    ) AS amount_usd,
    ez_lending_deposits_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
from deposits a
left join prices b
on a.token_address = b.token_address
    AND DATE_TRUNC(
        'hour',
        a.block_timestamp
    ) = b.hour