{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['pool_address'],
        merge_exclude_columns = ['inserted_timestamp'],
        post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(pool_address,token_a_mint,token_b_mint)'),
        tags = ['scheduled_non_core'],
    )
}}

{% set pool_platforms = [
    'raydiumv4', 
    'raydium_cpmm', 
    'raydium_clmm', 
    'orcav1', 
    'orcav2', 
    'orca_whirlpool', 
    'meteora',
    'meteora_dlmm'
] %}

WITH base AS (
{% for platform in pool_platforms %}
    SELECT 
        pool_address,
        pool_token_mint,
        token_a_mint,
        nullif(m.symbol,'') AS token_a_symbol,
        token_a_account,
        token_b_mint,
        nullif(m2.symbol,'') AS token_b_symbol,
        token_b_account,
        block_id AS initialized_at_block_id,
        block_timestamp AS initialized_at_block_timestamp,
        tx_id AS initialized_at_tx_id,
        index AS initialized_at_index,
        inner_index AS initialized_at_inner_index,
        program_id,
        CASE
            WHEN '{{ platform }}' ILIKE '%raydium%' THEN 'raydium'
            WHEN '{{ platform }}' ILIKE '%orca%' THEN 'orca'
            WHEN '{{ platform }}' ILIKE '%meteora%' THEN 'meteora'
        END AS platform,
        _inserted_timestamp,
        {{ 'initialization_pools_' ~ platform ~ '_id' }} AS liquidity_pools_id
    FROM
        {{ ref('silver__initialization_pools_' ~ platform) }} AS p
    LEFT JOIN
        {{ ref('price__ez_asset_metadata') }} AS m
        ON p.token_a_mint = m.token_address
    LEFT JOIN
        {{ ref('price__ez_asset_metadata') }} AS m2
        ON p.token_b_mint = m2.token_address
        {% if is_incremental() %}
        WHERE p._inserted_timestamp >= (SELECT max(_inserted_timestamp) FROM {{ this }})
        {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
)
    SELECT
        pool_address,
        CASE
            WHEN token_a_symbol IS NOT NULL 
            AND token_b_symbol IS NOT NULL THEN 
                token_a_symbol || '-' || token_b_symbol
            ELSE
                NULL
        END AS pool_name,
        pool_token_mint,
        token_a_mint,
        token_a_symbol,
        token_a_account,
        token_b_mint,
        token_b_symbol,
        token_b_account,
        initialized_at_block_id,
        initialized_at_block_timestamp,
        initialized_at_tx_id,
        initialized_at_index,
        initialized_at_inner_index,
        program_id,
        platform,
        liquidity_pools_id,
        _inserted_timestamp,
        sysdate() AS inserted_timestamp,
        sysdate() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        base AS b
    qualify
        row_number() over(PARTITION BY pool_address ORDER BY initialized_at_block_timestamp DESC) = 1