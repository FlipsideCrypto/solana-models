{{ config(
    materialized = 'incremental',
    unique_key = ['marinade_swaps_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}
{% if execute %}
    {% if is_incremental() %}
        {% set query %}
            SELECT MAX(_inserted_timestamp) AS max_inserted_timestamp
            FROM {{ this }}
        {% endset %}

        {% set max_inserted_timestamp = run_query(query)[0][0] %}
    {% endif %}
{% endif %}

{% set MSOL_MINT = 'mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So' %}
{% set MNDE_MINT = 'MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey' %}

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['tx_id','index','swap_index']) }} AS marinade_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_orca_view') }}
WHERE 
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    AND swap_to_amount IS NOT NULL
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    _inserted_timestamp,
    swaps_intermediate_meteora_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_meteora') }}
    WHERE 
        (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
        AND succeeded
        {% if is_incremental() %}
        AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
        {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    _inserted_timestamp,
    swaps_intermediate_phoenix_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_phoenix') }}
WHERE 
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    _inserted_timestamp,
    swaps_intermediate_bonkswap_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_bonkswap') }}
WHERE 
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    _inserted_timestamp,
    swaps_intermediate_raydium_v4_amm_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_raydium_v4_amm') }}
WHERE
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint,
    program_id,
    _inserted_timestamp,
    swaps_intermediate_raydium_cpmm_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_raydium_cpmm') }}
WHERE
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    _inserted_timestamp,
    swaps_intermediate_raydium_clmm_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_raydium_clmm') }}
WHERE
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint,
    program_id,
    _inserted_timestamp,
     swaps_intermediate_saber_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_saber') }}
WHERE
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint,
    program_id,
    _inserted_timestamp,
    swaps_pumpswap_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_pumpswap') }}
WHERE
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    _inserted_timestamp,
    swaps_intermediate_lifinity_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_lifinity') }}
WHERE
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    _inserted_timestamp,
    swaps_intermediate_orca_whirlpool_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_orca_whirlpool') }}
WHERE
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    from_amt AS swap_from_amount,
    from_mint AS swap_from_mint,
    to_amt AS swap_to_amount,
    to_mint AS swap_to_mint,
    program_id,
    _inserted_timestamp,
    swaps_intermediate_orca_token_swap_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_orca_token_swap') }}
WHERE
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}
UNION ALL
SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    index,
    inner_index,
    swap_index,
    swapper,
    swap_from_amount,
    swap_from_mint,
    swap_to_amount,
    swap_to_mint,
    program_id,
    _inserted_timestamp,
    swaps_intermediate_meteora_bonding_id as marinade_swaps_id,
    inserted_timestamp,
    modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__swaps_intermediate_meteora_bonding') }}
WHERE
    (swap_from_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}') OR swap_to_mint IN ('{{ MNDE_MINT }}', '{{ MSOL_MINT }}'))
    AND succeeded
    {% if is_incremental() %}
    AND _inserted_timestamp >= '{{ max_inserted_timestamp }}'
    {% endif %}