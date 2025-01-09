{{ config(
    materialized = 'incremental',
    meta = { 'database_tags': { 'table': { 'PURPOSE': 'SWAPS' }}},
    unique_key = ['marinade_ez_swaps_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id, swapper, swap_from_mint, swap_to_mint, program_id)'),
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

with swaps_jupiter AS (
    SELECT
        s.block_id,
        s.block_timestamp,
        s.tx_id,
        s.program_id,
        l.address_name AS platform,
        s.swapper,
        s.swap_from_mint,
        s.swap_from_amount,
        s.swap_to_mint,
        s.swap_to_amount,
        s.fact_swaps_jupiter_summary_id AS marinade_ez_swaps_id,
        s.inserted_timestamp,
        s.modified_timestamp
    FROM
        {{ ref('defi__fact_swaps_jupiter_summary') }} s
    LEFT OUTER JOIN {{ ref('core__dim_labels') }} l
        ON s.program_id = l.address
    WHERE 
        (swap_from_mint IN ('MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey', 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So') OR swap_to_mint IN ('MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey', 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'))
    {% if is_incremental() %}
    AND modified_timestamp >= '{{ max_modified_timestamp }}'
    {% endif %}
),

prices AS (
    SELECT
        hour,
        token_address,
        symbol,
        price
    FROM
        {{ ref('price__ez_prices_hourly') }}
),

swaps_jupiter_prices AS (
    SELECT
        d.block_id,
        d.block_timestamp,
        d.tx_id,
        d.swapper,
        d.swap_from_mint,
        p_in.symbol AS swap_from_symbol,
        d.swap_from_amount,
        ROUND(p_in.price * d.swap_from_amount, 2) AS swap_from_amount_usd,
        d.swap_to_mint,
        p_out.symbol AS swap_to_symbol,
        d.swap_to_amount,
        ROUND(p_out.price * d.swap_to_amount, 2) AS swap_to_amount_usd,
        program_id,
        platform,
        marinade_ez_swaps_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        swaps_jupiter d
    LEFT JOIN prices p_in
        ON d.swap_from_mint = p_in.token_address
        AND DATE_TRUNC('hour', d.block_timestamp) = p_in.hour
    LEFT JOIN prices p_out
        ON d.swap_to_mint = p_out.token_address
        AND DATE_TRUNC('hour', d.block_timestamp) = p_out.hour
),

swaps_non_agg AS (
    SELECT 
        block_id,
        block_timestamp,
        tx_id,
        swapper,
        swap_from_mint,
        swap_from_symbol,
        swap_from_amount,
        swap_from_amount_usd,
        swap_to_mint,
        swap_to_symbol,
        swap_to_amount,
        swap_to_amount_usd,
        program_id,
        swap_program AS platform,
        ez_swaps_id AS marinade_ez_swaps_id,
        inserted_timestamp,
        modified_timestamp
    FROM 
        {{ ref('defi__ez_dex_swaps') }}
    WHERE 
        (swap_from_mint IN ('MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey', 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So') OR swap_to_mint IN ('MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey', 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'))
        AND tx_id NOT IN (SELECT tx_id FROM swaps_jupiter)
    {% if is_incremental() %}
    AND 
        modified_timestamp >= '{{ max_modified_timestamp }}'
    {% endif %}
)

SELECT 
    block_id,
    block_timestamp,
    tx_id,
    swapper,
    swap_from_mint,
    swap_from_symbol,
    swap_from_amount,
    swap_from_amount_usd,
    swap_to_mint,
    swap_to_symbol,
    swap_to_amount,
    swap_to_amount_usd,
    program_id,
    platform,
    marinade_ez_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM swaps_non_agg
UNION 
SELECT 
    block_id,
    block_timestamp,
    tx_id,
    swapper,
    swap_from_mint,
    swap_from_symbol,
    swap_from_amount,
    swap_from_amount_usd,
    swap_to_mint,
    swap_to_symbol,
    swap_to_amount,
    swap_to_amount_usd,
    program_id,
    platform,
    marinade_ez_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM swaps_jupiter_prices