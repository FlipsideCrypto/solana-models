{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}},
    unique_key = ['fact_swaps_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, swapper, fact_swaps_id)'),
    tags = ['scheduled_non_core']
) }}

with swaps as (
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
    COALESCE (
       swaps_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_id','tx_id','program_id']
        ) }}
    ) AS fact_swaps_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__swaps') }}
/* TODO: DEPRECATE - remove jupiter swaps from this table, we will only cover individual dexes moving forward. Aggregator(s) get their own model(s) */
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
    swaps_intermediate_jupiterv6_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv6_view') }}
WHERE
    block_timestamp::date < '2023-08-03'
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
    swaps_intermediate_jupiterv6_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv6_2') }}
WHERE
    block_timestamp::date >= '2023-08-03'
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
    swaps_intermediate_jupiterv5_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv5_1_view') }}
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
    swaps_intermediate_jupiterv5_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv5_2_view') }}
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
    swaps_intermediate_bonkswap_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_bonkswap') }}
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
    swaps_intermediate_meteora_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_meteora') }}
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
    swaps_intermediate_dooar_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_dooar') }}
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
    swaps_intermediate_phoenix_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_phoenix') }}
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
    swaps_intermediate_raydium_clmm_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_raydium_clmm') }}
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
    swaps_intermediate_raydium_stable_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_raydium_stable') }}
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
    swaps_intermediate_raydium_v4_amm_id as fact_swaps_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_raydium_v4_amm') }}
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
    concat_ws('-',tx_id,swap_index,swap_program) as _log_id,
    fact_swaps_id,
    s.inserted_timestamp,
    s.modified_timestamp
FROM
    swaps
    s
    LEFT OUTER JOIN {{ ref('core__dim_labels') }}
    l
    ON s.program_id = l.address