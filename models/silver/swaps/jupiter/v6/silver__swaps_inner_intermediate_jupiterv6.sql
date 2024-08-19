 -- depends_on: {{ ref('silver__decoded_logs') }}

{{ config(
    materialized = 'incremental',
    unique_key = "swaps_inner_intermediate_jupiterv6_id",
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
        CREATE OR REPLACE TEMPORARY TABLE silver.swaps_inner_intermediate_jupiterv6__intermediate_tmp AS 
        WITH base AS (
            SELECT 
                tx_id
            FROM 
                {{ ref('silver__decoded_logs') }}
            WHERE
                program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
                AND event_type = 'SwapEvent'
                AND succeeded
                {% if is_incremental() %}
                AND _inserted_timestamp >= (
                    SELECT
                        MAX(_inserted_timestamp) - INTERVAL '1 hour'
                    FROM
                        {{ this }}
                )
                {% else %} 
                AND _inserted_timestamp::date >= '2024-06-12'
                AND _inserted_timestamp::date < '2024-06-14'
                {% endif %}
            {% if is_incremental() %}
            UNION ALL
            SELECT 
                l.tx_id
            FROM
                {{ this }} s 
            INNER JOIN 
                {{ ref('silver__decoded_logs') }} l
                ON s.block_timestamp::date = l.block_timestamp::date
                AND s.tx_id = l.tx_id
            WHERE
                l.program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
                AND l.event_type = 'SwapEvent'
                AND l.succeeded
                AND s.swapper IS NULL
                AND s._inserted_timestamp >= current_date - 2 /* only look back 2 days */
            {% endif %}
        ),
        /* we need to grab all inner_swaps for any tx that is in the incremental subset because it is required to do the window function later on */
        distinct_entities AS (
            SELECT
                DISTINCT tx_id
            FROM
                base
        )
        SELECT
            block_timestamp,
            block_id,
            tx_id,
            index,
            inner_index,
            succeeded,
            event_type,
            decoded_log:args:amm::string AS program_id,
            decoded_log:args:inputMint::string AS from_mint,
            decoded_log:args:inputAmount::string AS from_amount,
            decoded_log:args:outputMint::string AS to_mint,
            decoded_log:args:outputAmount::string AS to_amount,
            _inserted_timestamp,
        FROM
            {{ ref('silver__decoded_logs') }}
        JOIN
            distinct_entities
            USING(tx_id)
        WHERE 
            program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
            AND event_type = 'SwapEvent'
            AND succeeded
            /* need to always keep the upper bound (if there is one) to prevent time gaps in incremental loading */
            {% if is_incremental() %} 
            AND _inserted_timestamp < (
                SELECT
                    MAX(_inserted_timestamp) + INTERVAL '100 day'
                FROM
                    {{ this }}
            )
            {% else %} 
            AND _inserted_timestamp::date < '2024-06-14'
            {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.swaps_inner_intermediate_jupiterv6__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT 
        *
    FROM
        silver.swaps_inner_intermediate_jupiterv6__intermediate_tmp
    QUALIFY
        row_number() OVER (PARTITION BY tx_id, index, coalesce(inner_index,-1) ORDER BY _inserted_timestamp DESC) = 1
),
swappers AS (
    SELECT
        tx_id,
        index,
        inner_index,
        silver.udf_get_account_pubkey_by_name('userTransferAuthority', decoded_instruction:accounts) AS swapper,
        lead(inner_index) OVER (PARTITION BY tx_id, index ORDER BY inner_index) AS next_summary_swap_index,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        {{ between_stmts }}
        AND program_id = 'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4'
        AND event_type IN ('exactOutRoute','sharedAccountsExactOutRoute','sharedAccountsRoute','routeWithTokenLedger','route','sharedAccountsRouteWithTokenLedger')
        AND swapper IS NOT NULL
    QUALIFY
        row_number() OVER (PARTITION BY tx_id, index, coalesce(inner_index, -1) ORDER BY _inserted_timestamp DESC) = 1
),
token_decimals AS (
    SELECT 
        mint,
        decimal
    FROM
        {{ ref('silver__decoded_metadata') }}
    UNION ALL 
    SELECT 
        'So11111111111111111111111111111111111111112',
        9
    UNION ALL 
    SELECT 
        'GyD5AvrcZAhSP5rrhXXGPUHri6sbkRpq67xfG3x8ourT',
        9
),
pre_final AS (
    SELECT 
        b.block_timestamp,
        b.block_id,
        b.tx_id,
        b.index,
        b.inner_index,
        row_number() OVER (PARTITION BY b.tx_id, b.index, s.inner_index ORDER BY b.inner_index)-1 AS swap_index, /* we want the swap index as it relates to the top level swap instruction */
        b.succeeded,
        b.program_id AS swap_program_id,
        'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4' AS aggregator_program_id,
        s.swapper,
        b.from_mint,
        b.from_amount AS from_amount_int,
        b.from_amount * pow(10,-d.decimal) AS from_amount,
        b.to_mint,
        b.to_amount AS to_amount_int,
        b.to_amount * pow(10,-d2.decimal) AS to_amount,
        b._inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['b.tx_id','b.index','b.inner_index']) }} as swaps_inner_intermediate_jupiterv6_id,
        sysdate() as inserted_timestamp,
        sysdate() as modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM 
        base b
    LEFT OUTER JOIN
        swappers s
        ON b.tx_id = s.tx_id
        AND b.index = s.index
        AND (
                s.inner_index IS NULL 
                OR
                (
                    b.inner_index > s.inner_index
                    AND (b.inner_index < s.next_summary_swap_index 
                        OR s.next_summary_swap_index IS NULL)
                )
            )
    LEFT OUTER JOIN
        token_decimals d
        ON b.from_mint = d.mint
    LEFT OUTER JOIN
        token_decimals d2
        ON b.to_mint = d2.mint
),
distinct_missing_decimals AS (
    SELECT DISTINCT
        to_mint AS mint
    FROM
        pre_final
    WHERE
        to_amount IS NULL
    UNION
    SELECT DISTINCT
        from_mint
    FROM
        pre_final
    WHERE
        from_amount IS NULL
),
get_missing_decimals AS (
    SELECT
        mint,
        live.udf_api(
            'POST',
            '{service}/{Authentication}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),
            OBJECT_CONSTRUCT(
                'id',
                1,
                'jsonrpc',
                '2.0',
                'method',
                'getAccountInfo',
                'params',
                ARRAY_CONSTRUCT(
                    mint,
                    OBJECT_CONSTRUCT(
                        'encoding',
                        'jsonParsed'
                    )
                )
            ),
            'Vault/prod/solana/quicknode/mainnet'
        ):data:result:value:data:parsed:info:decimals::int AS decimal
    FROM
        distinct_missing_decimals
)
SELECT
    block_timestamp,
    block_id,
    tx_id,
    index,
    inner_index,
    swap_index, /* we want the swap index as it relates to the top level swap instruction */
    succeeded,
    swap_program_id,
    aggregator_program_id,
    swapper,
    from_mint,
    CASE
        WHEN from_amount IS NULL THEN
            from_amount_int * pow(10, -d.decimal)
        ELSE
            from_amount
    END AS from_amount,
    to_mint,
    CASE
        WHEN to_amount IS NULL THEN
            to_amount_int * pow(10, -d2.decimal)
        ELSE
            to_amount
    END AS to_amount,
    _inserted_timestamp,
    swaps_inner_intermediate_jupiterv6_id,
    inserted_timestamp,
    modified_timestamp,
    _invocation_id
FROM
    pre_final
LEFT OUTER JOIN
    get_missing_decimals d
    ON from_mint = d.mint
LEFT OUTER JOIN
    get_missing_decimals d2
    ON to_mint = d2.mint
