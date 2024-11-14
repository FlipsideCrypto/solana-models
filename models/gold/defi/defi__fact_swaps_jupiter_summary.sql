{{ config(
    materialized = 'incremental',
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}},
    unique_key = ['fact_swaps_jupiter_summary_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    cluster_by = ['block_timestamp::DATE','program_id'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = enable_search_optimization('{{this.schema}}','{{this.identifier}}','ON EQUALITY(tx_id, swapper, swap_from_mint, swap_to_mint, program_id, fact_swaps_jupiter_summary_id)'),
    tags = ['scheduled_non_core']
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

-- Select from the non-active models only during the initial FR
{% if not is_incremental() %}

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
    NULL as is_dca_swap,
    NULL as dca_requester,
    NULL as is_limit_swap,
    NULL as limit_requester,
    swaps_intermediate_jupiterv6_id AS fact_swaps_jupiter_summary_id,
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
    swap_index as index,
    NULL as inner_index,
    row_number() OVER (PARTITION BY tx_id ORDER BY index)-1 AS swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amt AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amt AS swap_to_amount,
    program_id,
    NULL as is_dca_swap,
    NULL as dca_requester,
    NULL as is_limit_swap,
    NULL as limit_requester,
    swaps_intermediate_jupiterv5_id as fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv5_1_view') }}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    swap_index as index,
    NULL as inner_index,
    row_number() OVER (PARTITION BY tx_id ORDER BY index)-1 AS swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amt AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amt AS swap_to_amount,
    program_id,
    NULL as is_dca_swap,
    NULL as dca_requester,
    NULL as is_limit_swap,
    NULL as limit_requester,
    swaps_intermediate_jupiterv5_id as fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv5_2_view') }}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    NULL as index,
    NULL as inner_index,
    0 as swap_index,
    succeeded,
    swapper,
    from_mint AS swap_from_mint,
    from_amt AS swap_from_amount,
    to_mint AS swap_to_mint,
    to_amt AS swap_to_amount,
    program_id,
    NULL as is_dca_swap,
    NULL as dca_requester,
    NULL as is_limit_swap,
    NULL as limit_requester,
    swaps_id as fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps') }}
WHERE
    program_id = 'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB'
    and block_timestamp::date <= '2023-10-31'
    and succeeded
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
    program_id,
    is_dca_swap,
    dca_requester,
    is_limit_swap,
    limit_requester,
    swaps_intermediate_jupiterv6_id AS fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv6_2') }}
WHERE
    block_timestamp::date >= '2023-08-03'
{% if is_incremental() %}
AND modified_timestamp >= '{{ max_modified_timestamp }}'
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
    NULL as is_dca_swap,
    NULL as dca_requester,
    NULL as is_limit_swap,
    NULL as limit_requester,
    swaps_intermediate_jupiterv4_id as fact_swaps_jupiter_summary_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__swaps_intermediate_jupiterv4_2') }}
WHERE
    block_timestamp::date > '2023-10-31'
    and not truncated_log
{% if is_incremental() %}
AND modified_timestamp >= '{{ max_modified_timestamp }}'
{% endif %}

