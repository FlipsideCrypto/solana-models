{{ config(
    materialized = 'incremental',
    meta = { 'database_tags': { 'table': { 'PURPOSE': 'STAKING' }}},
    unique_key = ['marinade_ez_liquid_staking_actions_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id, action_type, provider_address)'),
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

WITH liquid_staking_actions AS (
    SELECT 
        block_id,
        block_timestamp,
        tx_id,
        index,
        inner_index,
        action_type,
        provider_address,
        deposit_amount,
        msol_minted,
        msol_burned,
        claim_amount,
        program_id,
        _inserted_timestamp,
        marinade_liquid_staking_actions_id AS marinade_ez_liquid_staking_actions_id,
        inserted_timestamp,
        modified_timestamp
    FROM 
        {{ ref('silver__marinade_liquid_staking_actions') }}
    {% if is_incremental() %}
    WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
    {% endif %}
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
    a.block_id,
    a.block_timestamp,
    a.tx_id,
    a.index,
    a.inner_index,
    a.action_type,
    a.provider_address,
    a.deposit_amount,
    ROUND(b.price * a.deposit_amount, 2) AS deposit_amount_usd,
    a.msol_minted,
    a.msol_burned,
    a.claim_amount,
    ROUND(b.price * a.claim_amount, 2) AS claim_amount_usd,
    a.program_id,
    a.marinade_ez_liquid_staking_actions_id,
    a.inserted_timestamp,
    a.modified_timestamp
FROM
    liquid_staking_actions a
LEFT JOIN prices b
    ON a.action_type IN ('claim', 'deposit')
    AND b.token_address = 'So11111111111111111111111111111111111111112'
    AND DATE_TRUNC('hour', a.block_timestamp) = b.hour