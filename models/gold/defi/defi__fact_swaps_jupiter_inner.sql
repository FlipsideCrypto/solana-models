{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}},
    unique_key = ['fact_swaps_jupiter_inner_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','swap_program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, swapper, swap_from_mint, swap_to_mint, swap_program_id, aggregator_program_id, fact_swaps_jupiter_inner_id)'),
    tags = ['scheduled_non_core','scheduled_non_core_hourly']
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

-- Select from the non-active models only during the initial FR
{% if not is_incremental() %}
SELECT
    block_timestamp,
    block_id,
    tx_id,
    index,
    inner_index,
    swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amt AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amt AS swap_to_amount,
    inner_swap_program_id AS swap_program_id,
    program_id AS aggregator_program_id,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','inner_index']) }} AS fact_swaps_jupiter_inner_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv4_view') }}
WHERE
    block_timestamp <= '2023-10-31'
    and succeeded
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    index,
    inner_index,
    swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amount AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amount AS swap_to_amount,
    swap_program_id,
    aggregator_program_id,
    swaps_inner_intermediate_jupiterv5_id AS fact_swaps_jupiter_inner_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_inner_intermediate_jupiterv5') }}
UNION ALL
{% endif %}
-- Only select from active models during incremental
SELECT
    block_timestamp,
    block_id,
    tx_id,
    index,
    inner_index,
    swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amount AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amount AS swap_to_amount,
    swap_program_id,
    aggregator_program_id,
    swaps_inner_intermediate_jupiterv6_id AS fact_swaps_jupiter_inner_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_inner_intermediate_jupiterv6') }}
{% if is_incremental() %}
WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    index,
    inner_index,
    swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amount AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amount AS swap_to_amount,
    swap_program_id,
    aggregator_program_id,
    swaps_inner_intermediate_jupiterv4_id AS fact_swaps_jupiter_inner_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_inner_intermediate_jupiterv4') }}
WHERE 
    block_timestamp > '2023-10-31'
{% if is_incremental() %}
AND modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}

