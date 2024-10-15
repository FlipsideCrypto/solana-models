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
        CREATE OR REPLACE temporary TABLE silver.swaps_intermediate_jupiterv4__intermediate_tmp AS
        WITH distinct_entities AS (
            SELECT DISTINCT
                tx_id
            FROM 
                {{ ref('silver__decoded_instructions_combined') }}
            WHERE
                program_id = 'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB'
                AND event_type IN ('route', 'raydiumSwapExactOutput', 'raydiumClmmSwapExactOutput', 'whirlpoolSwapExactOutput')
                AND succeeded
            {% if is_incremental() %}
                AND _inserted_timestamp >= (
                    SELECT
                        MAX(_inserted_timestamp) - INTERVAL '1 hour'
                    FROM
                        {{ this }}
                )
                {% endif %}
        )

        SELECT
            block_timestamp,
            block_id,
            tx_id,
            INDEX,
            inner_index,
            succeeded,
            program_id,
            event_type,
            decoded_instruction,
            _inserted_timestamp
        FROM
            {{ ref('silver__decoded_instructions_combined') }}
        JOIN
            distinct_entities
            USING(tx_id)
        WHERE
            program_id = 'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB'
            AND event_type IN ('route', 'raydiumSwapExactOutput', 'raydiumClmmSwapExactOutput', 'whirlpoolSwapExactOutput')
            AND succeeded
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.swaps_intermediate_jupiterv4__intermediate_tmp", "block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.swaps_intermediate_jupiterv4__intermediate_tmp
),
route_events AS (
    SELECT
        d.block_timestamp,
        d.block_id,
        d.tx_id,
        d.index,
        d.inner_index,
        d.succeeded,
        d.program_id,
        d.event_type,
        _inserted_timestamp,
        p.key AS key,
        p.value AS VALUE,
        d.decoded_instruction
    FROM
        base d
        JOIN TABLE(FLATTEN(decoded_instruction :args :swapLeg :chain :swapLegs)) p
    WHERE
        event_type = 'route'
),
non_route_events AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        succeeded,
        program_id,
        event_type,
        _inserted_timestamp,
        decoded_instruction
    FROM
        base
    WHERE
        event_type IN (
            'raydiumSwapExactOutput',
            'raydiumClmmSwapExactOutput',
            'whirlpoolSwapExactOutput'
        )
),
all_routes AS (
    SELECT
        -- further flatten 'split' events
        i.block_timestamp,
        i.block_id,
        i.tx_id,
        i.index,
        i.inner_index,
        i.succeeded,
        i.program_id,
        k.key AS swap_index,
        i.event_type,
        LEAD(i.inner_index) over ( PARTITION BY i.tx_id, i.index ORDER BY i.inner_index) AS next_summary_swap_index_tmp,
        IFF(next_summary_swap_index_tmp = i.inner_index, NULL, next_summary_swap_index_tmp) AS next_summary_swap_index,
        MAX(i.key) over (PARTITION BY i.tx_id, i.index, i.inner_index) AS last_swap_index,
        i._inserted_timestamp,
        TRUE AS is_split
    FROM
        route_events i
        JOIN TABLE(FLATTEN(VALUE :split :splitLegs)) k
    UNION ALL
    SELECT
        i.block_timestamp,
        i.block_id,
        i.tx_id,
        i.index,
        i.inner_index,
        i.succeeded,
        i.program_id,
        i.key AS swap_index,
        i.event_type,
        LEAD(i.inner_index) over ( PARTITION BY i.tx_id, i.index ORDER BY i.inner_index) AS next_summary_swap_index_tmp,
        IFF(next_summary_swap_index_tmp = i.inner_index, NULL, next_summary_swap_index_tmp) AS next_summary_swap_index,
        MAX(i.key) over (PARTITION BY i.tx_id, i.index, i.inner_index) AS last_swap_index,
        i._inserted_timestamp,
        FALSE AS is_split
    FROM
        route_events i
    WHERE
        VALUE :split IS NULL
    UNION ALL
    SELECT
        -- separately get non-route events
        block_timestamp,
        block_id,
        tx_id,
        INDEX,
        inner_index,
        succeeded,
        program_id,
        0 AS swap_index,
        event_type,
        LEAD(inner_index) over (PARTITION BY tx_id, INDEX ORDER BY inner_index) AS next_summary_swap_index_tmp,
        IFF(next_summary_swap_index_tmp = inner_index, NULL, next_summary_swap_index_tmp) AS next_summary_swap_index,
        0 AS last_swap_index,
        _inserted_timestamp,
        FALSE AS is_split
    FROM
        non_route_events i
),
summary_base AS (
    SELECT
        r.block_timestamp,
        r.block_id,
        r.tx_id,
        r.index,
        r.inner_index,
        r.succeeded,
        r.program_id,
        r._inserted_timestamp
    FROM
        all_routes r
    WHERE
        swap_index = last_swap_index
),
summary_input_or_output_routes AS (
    SELECT
        r.*,
        CASE
            WHEN is_split THEN TRUE
            WHEN swap_index = 0 THEN TRUE
            ELSE FALSE
        END AS is_input_swap,
        CASE
            WHEN is_split THEN TRUE
            WHEN swap_index = last_swap_index THEN TRUE
            ELSE FALSE
        END AS is_output_swap,
    FROM
        all_routes r
        JOIN summary_base b
        ON r.tx_id = b.tx_id
        AND r.index = b.index
        AND COALESCE(r.inner_index,-1) = COALESCE(b.inner_index,-1)
    WHERE
        swap_index = last_swap_index
        OR swap_index = 0
        OR is_split
),
inner_swaps AS (
    SELECT
        block_timestamp,
        tx_id,
        INDEX,
        inner_index,
        swap_index,
        swapper,
        from_mint,
        from_amount,
        to_mint,
        to_amount
    FROM
        {{ ref('silver__swaps_inner_intermediate_jupiterv4') }}
    WHERE
        {{ between_stmts }}
),
input_swaps AS (
    SELECT
        s.tx_id,
        s.index,
        s.inner_index,
        swapper,
        from_mint AS mint,
        SUM(from_amount) AS amount
    FROM
        summary_input_or_output_routes s
        LEFT JOIN inner_swaps i
        ON i.block_timestamp :: DATE = s.block_timestamp :: DATE
        AND i.tx_id = s.tx_id
        AND i.index = s.index
        AND COALESCE(i.inner_index,-1) = COALESCE(s.inner_index,-1)
        AND i.swap_index = s.swap_index
    WHERE
        s.is_input_swap
    GROUP BY 1,2,3,4,5
),
output_swaps AS (
    SELECT
        s.tx_id,
        s.index,
        s.inner_index,
        to_mint AS mint,
        SUM(to_amount) AS amount
    FROM
        summary_input_or_output_routes s
        LEFT JOIN inner_swaps i
        ON i.block_timestamp :: DATE = s.block_timestamp :: DATE
        AND i.tx_id = s.tx_id
        AND i.index = s.index
        AND COALESCE(
            i.inner_index,
            -1
        ) = COALESCE(
            s.inner_index,
            -1
        )
        AND i.swap_index = s.swap_index
    WHERE
        s.is_output_swap
    GROUP BY 1,2,3,4
)
SELECT
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.index,
    b.inner_index,
    ROW_NUMBER() over (
        PARTITION BY b.tx_id
        ORDER BY
            b.index,
            b.inner_index
    ) -1 AS swap_index,
    b.succeeded,
    b.program_id,
    i.swapper,
    i.mint AS from_mint,
    i.amount AS from_amount,
    o.mint AS to_mint,
    o.amount AS to_amount,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['b.tx_id','b.index','b.inner_index']) }} AS swaps_intermediate_jupiterv4_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    summary_base b
    LEFT JOIN input_swaps i
    ON b.tx_id = i.tx_id
    AND b.index = i.index
    AND COALESCE(b.inner_index,-1) = COALESCE(i.inner_index,-1)
    LEFT JOIN output_swaps o
    ON b.tx_id = o.tx_id
    AND b.index = o.index
    AND COALESCE(b.inner_index,-1) = COALESCE(o.inner_index,-1)
