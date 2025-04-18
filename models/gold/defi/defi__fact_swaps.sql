{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}},
    unique_key = ['fact_swaps_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','swap_program'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, swapper, swap_from_mint, swap_to_mint, program_id, fact_swaps_id)'),
    tags = ['scheduled_non_core','scheduled_non_core_hourly']
) }}

{% if execute %}
    {% if is_incremental() %}
        {% set query %}
            SELECT MAX(modified_timestamp) AS max_modified_timestamp
            FROM {{ this }}
        {% endset %}

        {% set max_modified_timestamp = run_query(query).columns[0].values()[0] %}
    {% else %}
        {% set backfill_to_date = '2024-06-09' %}
    {% endif %}
{% endif %}

    -- Separate cte since the silver.swaps model has an existing _log_id
with swaps_general as (
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    _log_id,
    swaps_id AS fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps') }}
WHERE
    program_id != 'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB'
{% if is_incremental() %}
AND
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% else %}
AND
    modified_timestamp::date < '{{ backfill_to_date }}'
{% endif %}
)
,
swaps_individual as (
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    swap_index,
    swaps_intermediate_bonkswap_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_bonkswap') }}
{% if is_incremental() %}
WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
{% else %}
WHERE
    modified_timestamp::date < '{{ backfill_to_date }}'
{% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    swap_index,
    swaps_intermediate_meteora_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_meteora') }}
{% if is_incremental() %}
WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
{% else %}
WHERE
    modified_timestamp::date < '{{ backfill_to_date }}'
{% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    swap_index,
    swaps_intermediate_dooar_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_dooar') }}
{% if is_incremental() %}
WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
{% else %}
WHERE 
    modified_timestamp::date < '{{ backfill_to_date }}'
{% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    swap_index,
    swaps_intermediate_phoenix_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_phoenix') }}
{% if is_incremental() %}
WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
{% else %}
WHERE
    modified_timestamp::date < '{{ backfill_to_date }}'
{% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    swap_index,
    swaps_intermediate_raydium_clmm_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_raydium_clmm') }}
{% if is_incremental() %}
WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
{% else %}
WHERE
    modified_timestamp::date < '{{ backfill_to_date }}'
{% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    swap_index,
    swaps_intermediate_raydium_stable_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_raydium_stable') }}
{% if is_incremental() %}
WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
{% else %}
WHERE
    modified_timestamp::date < '{{ backfill_to_date }}'
{% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    swap_index,
    swaps_intermediate_raydium_v4_amm_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_raydium_v4_amm') }}
{% if is_incremental() %}
WHERE modified_timestamp >= '{{ max_modified_timestamp }}'
{% else %}
WHERE
    modified_timestamp::date < '{{ backfill_to_date }}'
{% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    from_amount AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amount AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    swap_index,
    swaps_pumpfun_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_pumpfun') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint,
    program_id,
    swap_index,
    swaps_intermediate_raydium_cpmm_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_raydium_cpmm') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint,
    program_id,
    swap_index,
    swaps_pumpswap_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_pumpswap') }}
{% if is_incremental() %}
WHERE
    modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}
)

select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint,
    program_id,
    l.address_name AS swap_program,
    _log_id,
    fact_swaps_id,
    s.inserted_timestamp,
    s.modified_timestamp
FROM
    swaps_general
    s
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    l
    ON s.program_id = l.address
union all
select 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint,
    program_id,
    l.address_name AS swap_program,
    concat_ws('-',tx_id,swap_index,swap_program) as _log_id,
    fact_swaps_id,
    s.inserted_timestamp,
    s.modified_timestamp
FROM
    swaps_individual
    s
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    l
    ON s.program_id = l.address