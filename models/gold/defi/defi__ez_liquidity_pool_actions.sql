{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        meta={'database_tags':{'table':{'PURPOSE': 'STAKING'}}},
        unique_key = ['ez_liquidity_pool_actions_id'],
        merge_exclude_columns = ['inserted_timestamp'],
        post_hook = enable_search_optimization(
            '{{this.schema}}',
            '{{this.identifier}}',
            'ON EQUALITY(ez_liquidity_pool_actions_id, pool_address, provider_address, tx_id, action_type, token_a_mint, token_b_mint)'),
        cluster_by = ['block_timestamp::DATE','action_type','program_id'],
        tags = ['scheduled_non_core'],
    )
}}

{% set pool_platforms = [
    'raydiumv4', 
    'raydium_cpmm', 
    'raydium_clmm',
    'raydiumstable',
    'orcav1', 
    'orcav2',
    'meteora_2',
    'meteora_dlmm_2',
    'orca_whirlpool',
] %}

WITH 
base AS (
    {% for platform in pool_platforms %}
        SELECT
            lp.block_id,
            lp.block_timestamp,
            lp.tx_id,
            lp.index,
            lp.inner_index,
            CASE
                WHEN REGEXP_LIKE(lp.event_type, '^(increase|add|deposit|bootstrap|open).*', 'i') THEN 'deposit'
                WHEN REGEXP_LIKE(lp.event_type, '^(decrease|remove|withdraw|close).*', 'i') THEN 'withdraw'
                ELSE lp.event_type
            END AS action_type,
            lp.provider_address,
            lp.token_a_mint,
            CASE
                WHEN lp.token_a_mint = m.token_a_mint THEN m.token_a_symbol
                WHEN lp.token_a_mint = m.token_b_mint THEN m.token_b_symbol
            END AS token_a_symbol,
            lp.token_a_amount,
            lp.token_b_mint,
            CASE
                WHEN lp.token_b_mint = m.token_a_mint THEN m.token_a_symbol
                WHEN lp.token_b_mint = m.token_b_mint THEN m.token_b_symbol
            END AS token_b_symbol,
            lp.token_b_amount,
            NULL AS token_c_mint,
            NULL AS token_c_symbol,
            NULL AS token_c_amount,
            NULL AS token_d_mint,
            NULL AS token_d_symbol,
            NULL AS token_d_amount,
            lp.pool_address,
            m.pool_name,
            lp.program_id,
            m.platform,
            lp.liquidity_pool_actions_{{ platform }}_id AS ez_liquidity_pool_actions_id
        FROM 
            {{ ref('silver__liquidity_pool_actions_' ~ platform) }} AS lp
        INNER JOIN
            {{ ref('silver__liquidity_pools') }} AS m
            USING(pool_address)
        {% if is_incremental() %}
            where lp.modified_timestamp > (select max(modified_timestamp) from {{ this }})
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}

    UNION ALL
    -- do meteora multi separately
        SELECT
            lp.block_id,
            lp.block_timestamp,
            lp.tx_id,
            lp.index,
            lp.inner_index,
            CASE
                WHEN REGEXP_LIKE(lp.event_type, '^(increase|add|deposit|bootstrap|open).*', 'i') THEN 'deposit'
                WHEN REGEXP_LIKE(lp.event_type, '^(decrease|remove|withdraw|close).*', 'i') THEN 'withdraw'
                ELSE lp.event_type
            END AS action_type,
            lp.provider_address,
            lp.token_a_mint,
            NULL AS token_a_symbol,
            lp.token_a_amount,
            lp.token_b_mint,
            NULL AS token_b_symbol,
            lp.token_b_amount,
            token_c_mint,
            NULL AS token_c_symbol,
            token_c_amount,
            token_d_mint,
            NULL AS token_d_symbol,
            token_d_amount,
            lp.pool_address,
            NULL AS pool_name,
            lp.program_id,
            'meteora' AS platform,
            lp.liquidity_pool_actions_meteora_multi_2_id AS ez_liquidity_pool_actions_id
        FROM 
            {{ ref('silver__liquidity_pool_actions_meteora_multi_2') }} AS lp
        {% if is_incremental() %}
            where lp.modified_timestamp > (select max(modified_timestamp) from {{ this }})
        {% endif %}
),
token_prices AS (
    SELECT
        HOUR,
        token_address,
        price,
        is_verified
    FROM
        {{ ref('price__ez_prices_hourly') }}
    WHERE
        HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                base
        )
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    index,
    inner_index,
    action_type,
    provider_address,
    token_a_mint,
    COALESCE(
        tp_a.is_verified,
        FALSE
    ) AS token_a_is_verified,
    token_a_symbol,
    token_a_amount,
    (token_a_amount * tp_a.price)::numeric(22,8) AS token_a_amount_usd,
    token_b_mint,
    COALESCE(
        tp_b.is_verified,
        FALSE
    ) AS token_b_is_verified,
    token_b_symbol,
    token_b_amount,
    (token_b_amount * tp_b.price)::numeric(22,8) AS token_b_amount_usd,
    token_c_mint,
    COALESCE(
        tp_c.is_verified,
        FALSE
    ) AS token_c_is_verified,
    token_c_symbol,
    token_c_amount,
    (token_c_amount * tp_c.price)::numeric(22,8) AS token_c_amount_usd,
    token_d_mint,
    COALESCE(
        tp_d.is_verified,
        FALSE
    ) AS token_d_is_verified,
    token_d_symbol,
    token_d_amount,
    (token_d_amount * tp_d.price)::numeric(22,8) AS token_d_amount_usd,
    pool_address,
    pool_name,
    program_id,
    platform,
    ez_liquidity_pool_actions_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM
    base AS b
LEFT JOIN
    token_prices AS tp_a
    ON date_trunc('hour', b.block_timestamp) = tp_a.HOUR
    AND b.token_a_mint = tp_a.token_address
LEFT JOIN
    token_prices AS tp_b
    ON date_trunc('hour', b.block_timestamp) = tp_b.HOUR
    AND b.token_b_mint = tp_b.token_address
LEFT JOIN
    token_prices AS tp_c
    ON date_trunc('hour', b.block_timestamp) = tp_c.HOUR
    AND b.token_c_mint = tp_c.token_address
LEFT JOIN
    token_prices AS tp_d
    ON date_trunc('hour', b.block_timestamp) = tp_d.HOUR
    AND b.token_d_mint = tp_d.token_address
