{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'BRIDGE' }}},
    unique_key = ['ez_bridge_activity_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'source_chain', 'destination_chain'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id,source_address,destination_address)'),
    tags = ['scheduled_non_core']
) }}

{% if execute %}

    {% if is_incremental() %}
        {% set query %}
            SELECT MAX(modified_timestamp) AS max_modified_timestamp
            FROM {{ this }}
        {% endset %}

        {% set max_modified_timestamp = run_query(query).columns[0].values()[0] %}
    {% endif %}
{% endif %}

WITH bridge AS (

    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        index,
        program_id,
        platform,
        direction,
        user_address,
        amount,
        mint,
        fact_bridge_activity_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('defi__fact_bridge_activity') }}
{% if is_incremental() %}
WHERE
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
                bridge
        )

)

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    INDEX,
    program_id,
    platform,
    direction,
    CASE
        WHEN direction = 'outbound' THEN 'solana'
        ELSE NULL
    END AS source_chain,
    CASE
        WHEN direction = 'inbound' THEN 'solana'
        ELSE NULL
    END AS destination_chain,
    CASE
        WHEN direction = 'outbound' THEN user_address
        ELSE NULL
    END AS source_address,
    CASE
        WHEN direction = 'inbound' THEN user_address
        ELSE NULL
    END AS destination_address,
    amount,
    amount * p.price AS amount_usd,
    mint,
    p.symbol as symbol,
    COALESCE(
        p.is_verified,
        FALSE
    ) AS token_is_verified,
    fact_bridge_activity_id as ez_bridge_activity_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM
    bridge a
    LEFT JOIN prices p
    ON a.mint = p.token_address
    AND DATE_TRUNC(
        'hour',
        a.block_timestamp
    ) = p.hour
{% if is_incremental() %}
where
    a.modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}