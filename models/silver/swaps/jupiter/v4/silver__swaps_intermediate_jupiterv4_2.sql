-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    unique_key = "swaps_intermediate_jupiterv4_id",
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
    post_hook = enable_search_optimization(
        '{{this.schema}}',
        '{{this.identifier}}',
        'ON EQUALITY(tx_id, swapper, from_mint, to_mint)'
    ),
    tags = ['scheduled_non_core'],
) }}

{% if execute %}
    {% set base_query %}
    CREATE
    OR REPLACE temporary TABLE silver.swaps_intermediate_jupiterv4__intermediate_tmp AS

    SELECT
        block_timestamp,
        block_id,
        succeeded,
        tx_id,
        INDEX,
        inner_index,
        program_id,
        decoded_instruction,
        event_type,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB'
        AND event_type IN (
            'route',
            'raydiumSwapExactOutput',
            'raydiumClmmSwapExactOutput',
            'whirlpoolSwapExactOutput'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '1 hour'
    FROM
        {{ this }}
)
{% else %}
    AND block_timestamp :: DATE between '2023-01-20' and '2023-05-01'
{% endif %}

{% endset %}
{% do run_query(base_query) %}
{% set between_stmts = fsc_utils.dynamic_range_predicate(
    "silver.swaps_intermediate_jupiterv4__intermediate_tmp",
    "block_timestamp::date"
) %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.swaps_intermediate_jupiterv4__intermediate_tmp
),
swaps AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        log_index,
        swap_index,
        swapper,
        from_mint,
        from_amount,
        to_mint,
        to_amount,
        _inserted_timestamp
    FROM
        {{ ref('silver__swaps_inner_intermediate_jupiterv4') }}
    WHERE
        {{ between_stmts }}
),
dest_swap AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        A.program_id,
        A.index,
        A.inner_index,
        b.log_index,
        b.swap_index,
        b.swapper,
        b.to_mint,
        b.to_amount,
        A._inserted_timestamp
    FROM
        base A
        LEFT JOIN swaps b
        ON A.block_timestamp :: DATE = b.block_timestamp :: DATE
        AND A.tx_id = b.tx_id
        AND A.index = b.index
        AND COALESCE(
            A.inner_index,
            -1
        ) = COALESCE(
            b.inner_index,
            -1
        ) qualify ROW_NUMBER() over (
            PARTITION BY b.tx_id,
            b.index,
            b.inner_index
            ORDER BY
                COALESCE(
                    b.swap_index,
                    -1
                ) DESC
        ) = 1
),
source_swap AS (
    SELECT
        A.block_timestamp,
        A.block_id,
        A.tx_id,
        A.succeeded,
        A.program_id,
        A.index,
        A.inner_index,
        b.log_index,
        b.swap_index,
        b.swapper,
        b.from_mint,
        b.from_amount,
        A._inserted_timestamp
    FROM
        base A
        LEFT JOIN swaps b
        ON A.block_timestamp :: DATE = b.block_timestamp :: DATE
        AND A.tx_id = b.tx_id
        AND A.index = b.index
        AND COALESCE(
            A.inner_index,
            -1
        ) = COALESCE(
            b.inner_index,
            -1
        )
    WHERE
        swap_index = 0
)
SELECT
    A.block_timestamp,
    A.block_id,
    A.tx_id,
    A.index,
    A.inner_index,
    ROW_NUMBER() over (
        PARTITION BY A.tx_id
        ORDER BY
            A.index,
            A.inner_index
    ) -1 AS swap_index,
    A.succeeded,
    A.program_id,
    A.swapper,
    A.from_mint,
    A.from_amount,
    b.to_mint,
    b.to_amount,
    A._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id','a.index','a.inner_index']) }} AS swaps_intermediate_jupiterv4_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    source_swap A
    LEFT JOIN dest_swap b
    ON A.tx_id = b.tx_id
    AND A.index = b.index
