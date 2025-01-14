-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = "swaps_intermediate_jupiterv6_id",
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
        CREATE OR REPLACE TEMPORARY TABLE silver.swaps_intermediate_jupiterv6__intermediate_tmp AS 
        WITH distinct_entities AS (
            SELECT DISTINCT
                tx_id
            FROM 
                {{ ref('silver__decoded_instructions_combined') }} d
            WHERE
                program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
                AND event_type IN ('exactOutRoute','sharedAccountsExactOutRoute','sharedAccountsRoute','routeWithTokenLedger','route','sharedAccountsRouteWithTokenLedger')
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
        /* need to re-select all decoded instructions from all tx_ids in incremental subset 
        in order for the window function to output accurate values */
        SELECT 
            d.block_timestamp,
            d.block_id,
            d.tx_id,
            d.index,
            d.inner_index,
            d.succeeded,
            d.program_id,
            p.key::int AS swap_index,
            d.event_type,
            lead(d.inner_index) OVER (PARTITION BY d.tx_id, d.index ORDER BY d.inner_index) AS next_summary_swap_index_tmp,
            iff(next_summary_swap_index_tmp = d.inner_index, NULL, next_summary_swap_index_tmp) AS next_summary_swap_index,
            max(p.key) OVER (PARTITION BY d.tx_id, d.index, d.inner_index) AS last_swap_index,
            p.value:inputIndex::int AS route_input_index,
            p.value:outputIndex::int AS route_output_index,
            _inserted_timestamp
        FROM 
            {{ ref('silver__decoded_instructions_combined') }} d
        JOIN
            distinct_entities
            USING(tx_id)
        JOIN
            table(flatten(decoded_instruction:args:routePlan)) p
        WHERE
            program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
            AND event_type IN ('exactOutRoute','sharedAccountsExactOutRoute','sharedAccountsRoute','routeWithTokenLedger','route','sharedAccountsRouteWithTokenLedger')
            AND succeeded
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.swaps_intermediate_jupiterv6__intermediate_tmp","block_timestamp::date") %}
{% endif %}

{% set jupiter_dca_signers = [
    'DCAKuApAuZtVNYLk3KTAVW9GLWVvPbnb5CxxRRmVgcTr',
    'DCAKxn5PFNN1mBREPWGdk1RXg5aVH9rPErLfBFEi2Emb',
    'DCAK36VfExkPdAkYUQg6ewgxyinvcEyPLyHjRbmveKFw',
    'BFQ2te7ERN319HA87mn6NJ9oxMUvNxyifqEhUWHFTie9',
    'JD1dHSqYkrXvqUVL8s6gzL1yB7kpYymsHfwsGxgwp55h',
    'JD38n7ynKYcgPpF7k1BhXEeREu1KqptU93fVGy3S624k',
    'JD25qVdtd65FoiXNmR89JjmoJdYk9sjYQeSTZAALFiMy'
] %}

{% set jupiter_limit_signers = [
    'j1oAbxxiDUWvoHxEDhWE7THLjEkDQW2cSHYn2vttxTF',
    'Gw9QoW4y72hFDVt3RRzyqcD4qrV4pSqjhMMzwdGunz6H',
    'LoAFmGjxUL84rWHk4X6k8jzrw12Hmb5yyReUXfkFRY6',
    '71WDyyCsZwyEYDV91Qrb212rdg6woCHYQhFnmZUBxiJ6',
    'EccxYg7rViwYfn9EMoNu7sUaV82QGyFt6ewiQaH1GYjv',
    'j1oeQoPeuEDmjvyMwBmCWexzCQup77kbKKxV59CnYbd',
    'JTJ9Cz7i43DBeps5PZdX1QVKbEkbWegBzKPxhWgkAf1',
    'j1opmdubY84LUeidrPCsSGskTCYmeJVzds1UWm6nngb',
    'AfQ1oaudsGjvznX4JNEw671hi57JfWo4CWqhtkdgoVHU'
] %}

WITH all_routes AS (
    SELECT 
        *
    FROM
        silver.swaps_intermediate_jupiterv6__intermediate_tmp
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
        0 AS input_index,
        route_output_index AS output_index,
        r._inserted_timestamp
    FROM
        all_routes r
    WHERE
        swap_index = last_swap_index
),
summary_input_or_ouput_routes AS (
    SELECT
        r.*,
        iff(route_input_index=0,TRUE,FALSE) AS is_input_swap,
        iff(route_output_index=output_index,TRUE,FALSE) AS is_output_swap -- handle situations where there is only 1 swap route (ie. it is both input and output swap)
    FROM
        all_routes r
    JOIN
        summary_base b
        ON r.tx_id = b.tx_id
        AND r.index = b.index
        AND coalesce(r.inner_index,-1) = coalesce(b.inner_index,-1)
    WHERE
        route_input_index = input_index
        OR route_output_index = output_index
),
inner_swaps AS (
    SELECT 
        block_timestamp,
        tx_id,
        index,
        inner_index,
        swap_index,
        swapper,
        from_mint,
        from_amount,
        to_mint,
        to_amount
    FROM
        {{ ref('silver__swaps_inner_intermediate_jupiterv6') }}
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
        sum(from_amount) AS amount
    FROM
        summary_input_or_ouput_routes s
    LEFT JOIN
        inner_swaps i
        ON i.block_timestamp::date = s.block_timestamp::date
        AND i.tx_id = s.tx_id
        AND i.index = s.index 
        AND i.swap_index = s.swap_index
        AND i.inner_index > coalesce(s.inner_index,-1)
        AND (i.inner_index < s.next_summary_swap_index 
             OR s.next_summary_swap_index IS NULL)
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
        sum(to_amount) AS amount
    FROM
        summary_input_or_ouput_routes s
    LEFT JOIN
        inner_swaps i
        ON i.block_timestamp::date = s.block_timestamp::date
        AND i.tx_id = s.tx_id
        AND i.index = s.index 
        AND i.swap_index = s.swap_index
        AND i.inner_index > coalesce(s.inner_index,-1)
        AND (i.inner_index < s.next_summary_swap_index 
             OR s.next_summary_swap_index IS NULL)
    WHERE
        s.is_output_swap
    GROUP BY 1,2,3,4
),
dca_filled AS (
    SELECT
        tx_id,
        decoded_log:args:userKey::string AS dca_requester,
    FROM 
        {{ ref('silver__decoded_logs') }}
    WHERE 
        {{ between_stmts }}
        AND program_id = 'DCA265Vj8a9CEuX1eb1LWRnDT7uK6q1xMipnNyatn23M'
        AND event_type = 'Filled'
),
limit_filled AS (
    SELECT
        tx_id,
        silver.udf_get_account_pubkey_by_name('maker', decoded_instruction:accounts) AS limit_requester,
    FROM 
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE 
        {{ between_stmts }}
        AND program_id in ('j1o2qRpjcyUwEvwtcfhEQefh773ZgjxcVRry7LDqg5X','jupoNjAxXgZ4rjzxzPMP4oxduvQsQtZzyknqvzYNrNu')
        AND event_type = 'flashFillOrder'
),
prefinal as (
SELECT 
    b.block_timestamp,
    b.block_id,
    b.tx_id,
    b.index,
    b.inner_index,
    row_number() OVER (PARTITION BY b.tx_id ORDER BY b.index, b.inner_index)-1 AS swap_index,
    b.succeeded,
    b.program_id,
    i.swapper,
    i.mint AS from_mint,
    i.amount AS from_amount,
    o.mint AS to_mint,
    o.amount AS to_amount,
    (
        i.swapper IN ('{{ jupiter_dca_signers | join("','") }}')
        AND d.tx_id IS NOT NULL
    ) AS is_dca_swap,
    d.dca_requester,
    (
        i.swapper IN ('{{ jupiter_limit_signers | join("','") }}')
        AND e.tx_id IS NOT NULL
    ) AS is_limit_swap,
    e.limit_requester,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['b.tx_id','b.index','b.inner_index']) }} AS swaps_intermediate_jupiterv6_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    summary_base b
LEFT JOIN 
    input_swaps i
    ON b.tx_id = i.tx_id
    AND b.index = i.index
    AND coalesce(b.inner_index,-1) = coalesce(i.inner_index,-1)
LEFT JOIN
    output_swaps o
    ON b.tx_id = o.tx_id
    AND b.index = o.index
    AND coalesce(b.inner_index,-1) = coalesce(o.inner_index,-1)
LEFT JOIN
    dca_filled d
    ON b.tx_id = d.tx_id
    LEFT JOIN
        limit_filled e
        ON b.tx_id = e.tx_id
)
SELECT *
FROM prefinal
qualify(row_number() over (partition by tx_id, index, inner_index order by swap_index desc)) = 1