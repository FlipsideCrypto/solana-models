-- depends_on: {{ ref('defi__fact_swaps_jupiter_summary') }}

{{ config(
    materialized = 'incremental',
    meta = { 'database_tags': { 'table': { 'PURPOSE': 'SWAPS', 'PROTOCOL': 'MARINADE' }}},
    unique_key = ['marinade_ez_swaps_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE', 'program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}', '{{this.identifier}}', 'ON EQUALITY(tx_id, swapper, swap_from_mint, swap_to_mint, program_id)'),
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.marinade_ez_swaps__intermediate_tmp AS 
        SELECT 
            block_timestamp,
            block_id,
            tx_id,
            index,
            inner_index,
            succeeded,
            swapper,
            swap_from_mint,
            swap_from_amount,
            swap_to_mint,
            swap_to_amount,
            program_id,
            fact_swaps_jupiter_summary_id AS marinade_ez_swaps_id,
            inserted_timestamp,
            modified_timestamp
        FROM
            {{ ref('defi__fact_swaps_jupiter_summary') }}
        WHERE
            (swap_from_mint IN ('MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey', 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So') 
            OR swap_to_mint IN ('MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey', 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'))
            {% if is_incremental() %}
            AND modified_timestamp >= (
                SELECT
                    MAX(modified_timestamp) - INTERVAL '3 hour'
                FROM
                    {{ this }}
            )
            {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.marinade_ez_swaps__intermediate_tmp", "block_timestamp::date") %}
{% endif %}

WITH jupiter_summary_swaps AS (
    SELECT 
        *
    FROM 
        silver.marinade_ez_swaps__intermediate_tmp
),

dex_swaps AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        index,
        inner_index,
        swapper,
        swap_from_amount,
        swap_from_mint,
        swap_to_amount,
        swap_to_mint,
        program_id,
        marinade_swaps_id AS marinade_ez_swaps_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver__marinade_swaps') }}
    WHERE 
        {{ between_stmts }}
),

jupiter_inner_swaps AS (
    SELECT DISTINCT 
        tx_id, 
        index
    FROM 
        {{ ref('defi__fact_swaps_jupiter_inner') }}
    WHERE 
        (swap_from_mint IN ('MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey', 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So') 
        OR swap_to_mint IN ('MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey', 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So'))
        AND succeeded
        AND {{ between_stmts }}
),

dex_swaps_excluding_jupiter_inner AS (
    SELECT 
        a.*
    FROM 
        dex_swaps a
    LEFT JOIN 
        (SELECT tx_id, index FROM jupiter_inner_swaps) b
    ON a.tx_id = b.tx_id
        AND a.index = b.index
    WHERE 
        b.tx_id IS NULL
),

combined AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        index,
        inner_index,
        swapper,
        swap_from_amount,
        swap_from_mint,
        swap_to_amount,
        swap_to_mint,
        program_id,
        marinade_ez_swaps_id,
        inserted_timestamp,
        modified_timestamp
    FROM 
        dex_swaps_excluding_jupiter_inner
    UNION ALL 
    SELECT 
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        index,
        inner_index,
        swapper,
        swap_from_amount,
        swap_from_mint,
        swap_to_amount,
        swap_to_mint,
        program_id,
        marinade_ez_swaps_id,
        inserted_timestamp,
        modified_timestamp
    FROM 
        jupiter_summary_swaps
),

prices AS (
    SELECT
        hour,
        token_address,
        symbol,
        price
    FROM
        {{ ref('price__ez_prices_hourly') }}
)

SELECT 
    c.block_timestamp,
    c.block_id,
    c.tx_id,
    c.index,
    c.inner_index,
    c.succeeded,
    c.swapper,
    c.swap_from_mint,
    p_in.symbol AS swap_from_symbol,
    c.swap_from_amount,
    ROUND(p_in.price * c.swap_from_amount, 2) AS swap_from_amount_usd,
    c.swap_to_mint,
    p_out.symbol AS swap_to_symbol,
    c.swap_to_amount,
    ROUND(p_out.price * c.swap_to_amount, 2) AS swap_to_amount_usd,
    c.program_id,
    l.address_name AS platform,
    c.marinade_ez_swaps_id,
    c.inserted_timestamp,
    c.modified_timestamp
from combined c
    LEFT OUTER JOIN {{ ref('core__dim_labels') }} l
        ON c.program_id = l.address
    LEFT JOIN prices p_in
        ON c.swap_from_mint = p_in.token_address
        AND DATE_TRUNC('hour', c.block_timestamp) = p_in.hour
    LEFT JOIN prices p_out
        ON c.swap_to_mint = p_out.token_address
        AND DATE_TRUNC('hour', c.block_timestamp) = p_out.hour

