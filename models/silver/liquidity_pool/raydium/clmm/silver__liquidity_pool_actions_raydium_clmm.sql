-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['block_timestamp::date','liquidity_pool_actions_raydium_clmm_id'],
        incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
        merge_exclude_columns = ["inserted_timestamp"],
        cluster_by = ['block_timestamp::date','modified_timestamp::date'],
        post_hook = enable_search_optimization(
            '{{this.schema}}',
            '{{this.identifier}}',
            'ON EQUALITY(tx_id, provider_address, token_a_mint, token_b_mint, liquidity_pool_actions_raydium_clmm_id)'
        ),
        tags = ['scheduled_non_core']
    )
}}

{% if execute %}
    {% if is_incremental() %}
        {% set max_timestamp_query %}
            SELECT max(_inserted_timestamp) FROM {{ this }}
        {% endset %}
        {% set max_timestamp = run_query(max_timestamp_query)[0][0] %}
    {% endif %}

    {% set base_query %}
    CREATE OR REPLACE TEMPORARY TABLE silver.liquidity_pool_actions_raydium_clmm__intermediate_tmp AS 
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        index,
        inner_index,
        succeeded,
        event_type,
        decoded_instruction:accounts AS accounts,
        decoded_instruction:args AS args,
        program_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined')}}
    WHERE
        program_id = 'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK'
        AND event_type IN (
            'closePosition',
            'decreaseLiquidity',
            'decreaseLiquidityV2',
            'increaseLiquidity',
            'increaseLiquidityV2',
            'openPositionV2'
            /* 'openPosition' - This v1 event does not actually add liquidity to the pool, it only creates a mint representing the position.
                                Leaving this note here so we know why we didnt include this event type in this model
            */
        )
        AND succeeded
        {% if is_incremental() %}
        -- AND _inserted_timestamp > '{{ max_timestamp }}'
        /* batches for reload */
        -- AND block_timestamp::date BETWEEN '2023-01-01' AND '2023-06-01'
        -- AND block_timestamp::date BETWEEN '2023-06-01' AND '2024-01-01'
        AND block_timestamp::date BETWEEN '2024-01-01' AND '2024-06-01'
        -- AND block_timestamp::date BETWEEN '2024-06-01' AND '2025-01-05'
        -- AND _inserted_timestamp > '{{ max_timestamp }}'::timestamp_ntz - INTERVAL '1 DAY'
        {% else %}
        AND block_timestamp::date BETWEEN '2022-08-17' AND '2023-01-01'
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.liquidity_pool_actions_raydium_clmm__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT 
        * exclude(accounts),
        silver.udf_get_account_pubkey_by_name('poolState', accounts) AS pool_address,
        CASE
            WHEN event_type = 'openPositionV2' THEN
                silver.udf_get_account_pubkey_by_name('positionNftOwner', accounts)
            ELSE
                silver.udf_get_account_pubkey_by_name('nftOwner', accounts)
        END AS provider_address,
        silver.udf_get_account_pubkey_by_name('tokenVault0', accounts) AS pool_token_a_account,
        silver.udf_get_account_pubkey_by_name('tokenVault1', accounts) AS pool_token_b_account,
        CASE
            WHEN event_type IN ('decreaseLiquidity', 'decreaseLiquidityV2', 'closePosition') THEN
                silver.udf_get_account_pubkey_by_name('recipientTokenAccount0', accounts)
            ELSE
                silver.udf_get_account_pubkey_by_name('tokenAccount0', accounts)
        END AS token_a_account,
        CASE
            WHEN event_type IN ('decreaseLiquidity', 'decreaseLiquidityV2', 'closePosition') THEN
                silver.udf_get_account_pubkey_by_name('recipientTokenAccount1', accounts)
            ELSE
                silver.udf_get_account_pubkey_by_name('tokenAccount1', accounts)
        END AS token_b_account,
        coalesce(lead(inner_index) OVER (
            PARTITION BY tx_id, index 
            ORDER BY inner_index
        ), 9999) AS next_lp_action_inner_index
    FROM 
        silver.liquidity_pool_actions_raydium_clmm__intermediate_tmp
    WHERE
        (
            /* exclude closed positions with less than 6 accountsbecause they are preceeded by a decreaseLiquidity event that removes all liquidity from the position */
            (event_type = 'closePosition' 
            AND array_size(accounts) > 6)
            OR
            /* exclude any decreaseLiquidity events that take out 0 tokens from the pool */
            (event_type IN ('decreaseLiquidity', 'decreaseLiquidityV2')
            AND (args:amount0Min::int > 0 OR args:amount1Min::int > 0))
            OR
            /* exclude any openPositionV2 events that show 0 liquidity */
            (event_type = 'openPositionV2'
            AND (args:liquidity::int > 0))
            OR
            (event_type NOT IN ('closePosition', 'decreaseLiquidity', 'decreaseLiquidityV2', 'openPositionV2'))
        )
),

transfers AS (
    SELECT 
        * exclude(index),
        split_part(index,'.',1)::int AS index,
        nullif(split_part(index,'.',2),'')::int AS inner_index
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        succeeded
        AND {{ between_stmts }}
),

deposit_transfers AS (
    SELECT 
        b.*,
        t.mint AS token_a_mint,
        t.amount AS token_a_amount,
        t2.mint AS token_b_mint,
        t2.amount AS token_b_amount
    FROM 
        base AS b
    LEFT JOIN
        transfers AS t
        ON t.block_timestamp::date = b.block_timestamp::date
        AND t.tx_id = b.tx_id
        AND t.index = b.index
        AND coalesce(t.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND t.source_token_account = b.token_a_account
        AND t.dest_token_account = b.pool_token_a_account
    LEFT JOIN
        transfers AS t2
        ON t2.block_timestamp::date = b.block_timestamp::date
        AND t2.tx_id = b.tx_id
        AND t2.index = b.index
        AND coalesce(t2.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t2.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND t2.source_token_account = b.token_b_account
        AND t2.dest_token_account = b.pool_token_b_account
    WHERE
        b.event_type IN (
            'increaseLiquidity',
            'increaseLiquidityV2',
            'openPositionV2'
        )
    QUALIFY
        row_number() OVER (PARTITION BY b.tx_id, b.index, b.inner_index ORDER BY t.inner_index, t2.inner_index) = 1
),

withdraw_transfers AS (
    SELECT 
        b.*,
        t.mint AS token_a_mint,
        t.amount AS token_a_amount,
        t2.mint AS token_b_mint,
        t2.amount AS token_b_amount
    FROM 
        base AS b
    LEFT JOIN
        transfers AS t
        ON t.block_timestamp::date = b.block_timestamp::date
        AND t.tx_id = b.tx_id
        AND t.index = b.index
        AND coalesce(t.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND t.dest_token_account = b.token_a_account
        AND t.source_token_account = b.pool_token_a_account
    LEFT JOIN
        transfers AS t2
        ON t2.block_timestamp::date = b.block_timestamp::date
        AND t2.tx_id = b.tx_id
        AND t2.index = b.index
        AND coalesce(t2.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t2.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND t2.dest_token_account = b.token_b_account
        AND t2.source_token_account = b.pool_token_b_account
    WHERE
        b.event_type IN (
            'closePosition',
            'decreaseLiquidity',
            'decreaseLiquidityV2'
        )
    QUALIFY
        row_number() OVER (PARTITION BY b.tx_id, b.index, b.inner_index ORDER BY t.inner_index, t2.inner_index) = 1
),

pre_final AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        index,
        inner_index,
        succeeded,
        event_type,
        pool_address,
        provider_address,
        token_a_mint,
        token_a_amount,
        token_b_mint,
        token_b_amount,
        program_id,
        _inserted_timestamp
    FROM 
        deposit_transfers

    UNION ALL

    SELECT 
        block_id,
        block_timestamp,
        tx_id,
        index,
        inner_index,
        succeeded,
        event_type,
        pool_address,
        provider_address,
        token_a_mint,
        token_a_amount,
        token_b_mint,
        token_b_amount,
        program_id,
        _inserted_timestamp
    FROM 
        withdraw_transfers
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    index,
    inner_index,
    succeeded,
    event_type,
    pool_address,
    provider_address,
    /* 
    mimic behavior of our other liquidity pool action models that support single token withdraws/deposits.
    Represent the single token action as token A
    */
    iff(token_a_mint IS NULL AND token_a_amount IS NULL, token_b_mint, token_a_mint) AS token_a_mint,
    iff(token_a_mint IS NULL AND token_a_amount IS NULL, token_b_amount, token_a_amount) AS token_a_amount,
    iff(token_a_mint IS NOT NULL OR token_a_amount IS NOT NULL, token_b_mint, NULL) AS token_b_mint,
    iff(token_a_mint IS NOT NULL OR token_a_amount IS NOT NULL, token_b_amount, NULL) AS token_b_amount,
    program_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_raydium_clmm_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final
WHERE
    (
        /* 
        There are 11 decreaseLiquidity events on `2022-11-06` that have 0 transfers while listing a non-zero amount in the instruction args 
        Omit these events from the model
        */
        (block_timestamp::date = '2022-11-06'
        AND token_a_mint IS NOT NULL)
        OR
        (block_timestamp::date <> '2022-11-06')
    )

