{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}},
    unique_key = ['fact_swaps_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, swapper, fact_swaps_jupiter_summary_id)'),
    tags = ['scheduled_non_core']
) }}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    swap_index AS index,
    NULL AS inner_index,
    swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amt AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amt AS swap_to_amount,
    program_id,
    swaps_intermediate_jupiterv6_id AS fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__swaps_intermediate_jupiterv6_view') }}
WHERE
    block_timestamp::date < '2023-08-03'
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
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
    program_id,
    swaps_intermediate_jupiterv6_id AS fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp,
FROM
    {{ ref('silver__swaps_intermediate_jupiterv6_2') }}
WHERE
    block_timestamp::date >= '2023-08-03'
{% if is_incremental() %}
AND modified_timestamp >= (
    SELECT
        MAX(modified_timestamp)
    FROM
        {{ this }}
)
{% endif %}