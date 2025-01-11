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
        unique_key = ['pool_address'],
        merge_exclude_columns = ['inserted_timestamp'],
        post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(pool_address,token_a_mint,token_b_mint)'),
        tags = ['scheduled_non_core'],
    )
}}

/*
    TODO: add these when they have been deployed
    'orca_whirlpool', 
    'meteora',
    'meteora_dlmm'
*/
{% set pool_platforms = [
    'raydiumv4', 
    'raydium_cpmm', 
    'raydium_clmm', 
    'orcav1', 
    'orcav2', 
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
        (
            token_a_mint = 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So' 
            OR token_b_mint = 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'
        ) AS is_msol_pool,
        (
            token_a_mint = 'MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey' 
            OR token_b_mint = 'MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey'
        ) AS is_mnde_pool,
        program_id,
        CASE
            WHEN '{{ platform }}' ILIKE '%raydium%' THEN 'raydium'
            WHEN '{{ platform }}' ILIKE '%orca%' THEN 'orca'
            WHEN '{{ platform }}' ILIKE '%meteora%' THEN 'meteora'
        END AS platform,
        {{ 'initialization_pools_' ~ platform ~ '_id' }} AS dim_pools_id
    FROM
        {{ ref('silver__initialization_pools_' ~ platform) }} AS p
    LEFT JOIN
        {{ ref('price__ez_asset_metadata') }} AS m
        ON p.token_a_mint = m.token_address
    LEFT JOIN
        {{ ref('price__ez_asset_metadata') }} AS m2
        ON p.token_b_mint = m2.token_address
    WHERE
        (
            token_a_mint IN ('mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So','MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey')
            OR token_b_mint IN ('mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So','MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey')
        )
        {% if is_incremental() %}
            AND p.modified_timestamp > (SELECT max(modified_timestamp) FROM {{ this }})
        {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
),

fill_null_symbols AS (
    SELECT
        pool_address,
        live.udf_api(
            'GET',
            concat('{Service}/token/meta?address=',token_a_mint),
            object_construct(
                'Content-Type',
                'application/json',
                'token',
                '{Authentication}'
            ),
            {},
            'Vault/prod/solana/solscan/v2'
        ):data:data:symbol::string AS token_a_symbol,
        live.udf_api(
            'GET',
            concat('{Service}/token/meta?address=',token_b_mint),
            object_construct(
                'Content-Type',
                'application/json',
                'token',
                '{Authentication}'
            ),
            {},
            'Vault/prod/solana/solscan/v2'
        ):data:data:symbol::string AS token_b_symbol
    FROM
        base
    WHERE
        (token_a_symbol IS NULL
        OR token_b_symbol IS NULL)
),

pre_final AS (
    SELECT
        b.pool_address,
        pool_token_mint,
        token_a_mint,
        coalesce(b.token_a_symbol,s.token_a_symbol) AS token_a_symbol,
        token_a_account,
        token_b_mint,
        coalesce(b.token_b_symbol,s.token_b_symbol) AS token_b_symbol,
        token_b_account,
        initialized_at_block_id,
        initialized_at_block_timestamp,
        initialized_at_tx_id,
        initialized_at_index,
        initialized_at_inner_index,
        is_msol_pool,
        is_mnde_pool,
        program_id,
        platform,
        dim_pools_id
    FROM
        base AS b
    LEFT JOIN
        fill_null_symbols AS s
        ON b.pool_address = s.pool_address
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
    is_msol_pool,
    is_mnde_pool,
    program_id,
    platform,
    dim_pools_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp
FROM
    pre_final