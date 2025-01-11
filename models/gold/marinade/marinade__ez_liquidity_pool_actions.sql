{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        meta={
            'database_tags':{
                'table': {
                    'PROTOCOL': 'MARINADE',
                    'PURPOSE': 'STAKING'
                }
            }
        },
        unique_key = ['ez_liquidity_pool_actions_id'],
        merge_exclude_columns = ['inserted_timestamp'],
        post_hook = enable_search_optimization(
            '{{this.schema}}',
            '{{this.identifier}}',
            'ON EQUALITY(ez_liquidity_pool_actions_id, pool_address, provider_address, tx_id, action, token_a_mint, token_b_mint)'),
        tags = ['scheduled_non_core'],
    )
}}

/*
    TODO: add these when they have been deployed
    'orca_whirlpool', 
*/
{% set pool_platforms = [
    'raydiumv4', 
    'raydium_cpmm', 
    'raydium_clmm', 
    'orcav1', 
    'orcav2',
    'meteora',
    'meteora_dlmm',
] %}

WITH marinade_pool_tokens AS (
    SELECT DISTINCT
        token_a_mint AS token_address
    FROM
        {{ ref('marinade__dim_pools') }}
    UNION
    SELECT DISTINCT
        token_b_mint
    FROM
        {{ ref('marinade__dim_pools') }}
),
base AS (
    {% for platform in pool_platforms %}
        select
            lp.block_id,
            lp.block_timestamp,
            lp.tx_id,
            lp.index,
            lp.inner_index,
            case
                WHEN REGEXP_LIKE(lp.event_type, '^(increase|add|deposit|bootstrap|open).*', 'i') THEN 
                    'deposit'
                WHEN REGEXP_LIKE(lp.event_type, '^(decrease|remove|withdraw|close).*', 'i') THEN 
                    'withdraw'
                ELSE
                    lp.event_type
            END AS action_type,
            lp.provider_address,
            lp.token_a_mint,
            case
                when lp.token_a_mint = m.token_a_mint then
                    m.token_a_symbol
                when lp.token_a_mint = m.token_b_mint then
                    m.token_b_symbol
            end AS token_a_symbol,
            lp.token_a_amount,
            lp.token_b_mint,
            case
                when lp.token_b_mint = m.token_a_mint then
                    m.token_a_symbol
                when lp.token_b_mint = m.token_b_mint then
                    m.token_b_symbol
            end AS token_b_symbol,
            lp.token_b_amount,
            lp.pool_address,
            m.pool_name,
            m.is_msol_pool,
            m.is_mnde_pool,
            lp.program_id,
            m.platform,
            lp.liquidity_pool_actions_{{ platform }}_id AS ez_liquidity_pool_actions_id
        from 
            {% if platform == 'meteora' or platform == 'meteora_dlmm' %}
            {{ ref('marinade__' ~ platform ~ '_pivot') }} AS lp
            {% else %}
            {{ ref('silver__liquidity_pool_actions_' ~ platform) }} AS lp
            {% endif %}
        inner join
            {{ ref('marinade__dim_pools') }} AS m
            using(pool_address)
        {% if is_incremental() %}
            where lp.modified_timestamp > (select max(modified_timestamp) from {{ this }})
        {% endif %}
        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor %}
),
token_prices AS (
    SELECT
        HOUR,
        p.token_address,
        price
    FROM
        {{ ref('price__ez_prices_hourly') }} AS p
    INNER JOIN
        marinade_pool_tokens AS m
        ON p.token_address = m.token_address
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
    token_a_symbol,
    token_a_amount,
    (token_a_amount * tp_a.price)::numeric(20,8) AS token_a_amount_usd,
    token_b_mint,
    token_b_symbol,
    token_b_amount,
    (token_b_amount * tp_b.price)::numeric(20,8) AS token_b_amount_usd,
    pool_address,
    pool_name,
    is_msol_pool,
    is_mnde_pool,
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
