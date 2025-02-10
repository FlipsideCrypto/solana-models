-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['block_timestamp::date','liquidity_pool_actions_meteora_multi_2_id'],
        incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
        merge_exclude_columns = ["inserted_timestamp"],
        cluster_by = ['block_timestamp::date','modified_timestamp::date'],
        post_hook = enable_search_optimization(
            '{{this.schema}}',
            '{{this.identifier}}',
            'ON EQUALITY(tx_id, provider_address, token_a_mint, token_b_mint, liquidity_pool_actions_meteora_multi_2_id)'
        ),
        tags = ['scheduled_non_core']
    )
}}

{% set batch_backfill_size = var('batch_backfill_size', 0) %}
{% set batch_backfill = False if batch_backfill_size == 0 else True %}



{% if execute %}
    {% if is_incremental() %}
        {% set max_timestamp_query %}
            SELECT max(_inserted_timestamp) FROM {{ this }}
        {% endset %}
        {% set max_timestamp = run_query(max_timestamp_query)[0][0] %}
    {% endif %}

    {% set base_query %}
    CREATE OR REPLACE TEMPORARY TABLE silver.liquidity_pool_actions_meteora_multi_2__intermediate_tmp AS 
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
        program_id = 'MERLuDFBMmsHnsBPZw2sDQZHvXFMwp8EdjudcU2HKky'
        AND event_type IN (
            'addLiquidity',
            'removeLiquidity',
            'removeLiquidityOneToken'
        )
        AND succeeded
        {% if is_incremental() and not batch_backfill %}
        AND _inserted_timestamp > '{{ max_timestamp }}'
        /* batches for reload */
        {% elif batch_backfill %}
            {% set max_block_ts_query %}
                SELECT max(_inserted_timestamp)::date FROM {{ this }}
            {% endset %}
            {% set max_block_ts = run_query(max_block_ts_query)[0][0] %}
            {% set end_date = max_block_ts + modules.datetime.timedelta(days=batch_backfill_size) %}
            AND _inserted_timestamp::date BETWEEN '{{ max_block_ts }}' AND '{{ end_date }}'
        {% else %}
        AND _inserted_timestamp::date BETWEEN '2024-12-01' AND '2025-03-01'
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.liquidity_pool_actions_meteora_multi_2__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT 
        * exclude(accounts),
        silver.udf_get_account_pubkey_by_name('stableSwap', accounts) AS pool_address,
        silver.udf_get_account_pubkey_by_name('userTransferAuthority', accounts) AS provider_address,
        silver.udf_get_account_pubkey_by_name('Remaining 0', accounts) AS pool_token_a_account,
        silver.udf_get_account_pubkey_by_name('Remaining 1', accounts) AS pool_token_b_account,
        CASE
            WHEN event_type = 'removeLiquidityOneToken' THEN
                iff(array_size(accounts) >= 10,silver.udf_get_account_pubkey_by_name('Remaining 2', accounts),NULL) 
            ELSE
                iff(array_size(accounts) >= 12,silver.udf_get_account_pubkey_by_name('Remaining 2', accounts),NULL) 
        END AS pool_token_c_account,
        CASE
            WHEN event_type = 'removeLiquidityOneToken' THEN
                iff(array_size(accounts) >= 11,silver.udf_get_account_pubkey_by_name('Remaining 3', accounts),NULL) 
            ELSE
                iff(array_size(accounts) >= 14,silver.udf_get_account_pubkey_by_name('Remaining 3', accounts),NULL)
        END AS pool_token_d_account,
        CASE
            WHEN (array_size(accounts) = 10 AND event_type <> 'removeLiquidityOneToken') OR (event_type = 'removeLiquidityOneToken' AND array_size(accounts) = 9) THEN
                silver.udf_get_account_pubkey_by_name('Remaining 3', accounts)
            WHEN (array_size(accounts) = 12 AND event_type <> 'removeLiquidityOneToken') OR (event_type = 'removeLiquidityOneToken' AND array_size(accounts) = 10) THEN
                silver.udf_get_account_pubkey_by_name('Remaining 4', accounts)
            WHEN (array_size(accounts) = 14 AND event_type <> 'removeLiquidityOneToken') OR (event_type = 'removeLiquidityOneToken' AND array_size(accounts) = 11) THEN
                silver.udf_get_account_pubkey_by_name('Remaining 5', accounts)
            ELSE
                NULL
        END AS token_a_account,
        CASE
            WHEN event_type = 'removeLiquidityOneToken' THEN
                NULL
            WHEN array_size(accounts) = 10 THEN
                silver.udf_get_account_pubkey_by_name('Remaining 4', accounts)
            WHEN array_size(accounts) = 12 THEN
                silver.udf_get_account_pubkey_by_name('Remaining 5', accounts)
            WHEN array_size(accounts) = 14 THEN
                silver.udf_get_account_pubkey_by_name('Remaining 6', accounts)
            ELSE
                NULL
        END AS token_b_account,
        CASE
            WHEN event_type = 'removeLiquidityOneToken' THEN
                NULL
            WHEN array_size(accounts) = 12 THEN
                silver.udf_get_account_pubkey_by_name('Remaining 6', accounts)
            WHEN array_size(accounts) = 14 THEN
                silver.udf_get_account_pubkey_by_name('Remaining 7', accounts)
            ELSE
                NULL
        END AS token_c_account,
        CASE
            WHEN event_type = 'removeLiquidityOneToken' THEN
                NULL
            WHEN array_size(accounts) = 14 THEN
                silver.udf_get_account_pubkey_by_name('Remaining 8', accounts)
            ELSE
                NULL
        END AS token_d_account,
        coalesce(lead(inner_index) OVER (
            PARTITION BY tx_id, index 
            ORDER BY inner_index
        ), 9999) AS next_lp_action_inner_index
    FROM 
        silver.liquidity_pool_actions_meteora_multi_2__intermediate_tmp
),

transfers AS (
    SELECT 
        t.* exclude(index),
        split_part(t.index,'.',1)::int AS index,
        nullif(split_part(t.index,'.',2),'')::int AS inner_index
    FROM
        {{ ref('silver__transfers') }} AS t
    INNER JOIN
        (SELECT DISTINCT block_timestamp::date AS bt, tx_id FROM base) AS b
        ON b.bt = t.block_timestamp::date
        AND b.tx_id = t.tx_id
    WHERE
        t.succeeded
        AND {{ between_stmts }}
),

deposit_transfers AS (
    SELECT 
        b.* exclude(args),
        t.mint AS token_a_mint,
        t.amount AS token_a_amount,
        t2.mint AS token_b_mint,
        t2.amount AS token_b_amount,
        t3.mint AS token_c_mint,
        t3.amount AS token_c_amount,
        t4.mint AS token_d_mint,
        t4.amount AS token_d_amount
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
    LEFT JOIN
        transfers AS t3
        ON t3.block_timestamp::date = b.block_timestamp::date
        AND t3.tx_id = b.tx_id
        AND t3.index = b.index
        AND coalesce(t3.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t3.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND b.token_c_account IS NOT NULL
        AND t3.source_token_account = b.token_c_account
        AND t3.dest_token_account = b.pool_token_c_account
    LEFT JOIN
        transfers AS t4
        ON t4.block_timestamp::date = b.block_timestamp::date
        AND t4.tx_id = b.tx_id
        AND t4.index = b.index
        AND coalesce(t4.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t4.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND b.token_d_account IS NOT NULL
        AND t4.source_token_account = b.token_d_account
        AND t4.dest_token_account = b.pool_token_d_account
    WHERE
        b.event_type = 'addLiquidity'
    QUALIFY
        row_number() OVER (PARTITION BY b.tx_id, b.index, b.inner_index ORDER BY t.inner_index, t2.inner_index, t3.inner_index, t4.inner_index) = 1
),

withdraw_transfers AS (
    SELECT 
        b.* exclude(args),
        t.mint AS token_a_mint,
        t.amount AS token_a_amount,
        t2.mint AS token_b_mint,
        t2.amount AS token_b_amount,
        t3.mint AS token_c_mint,
        t3.amount AS token_c_amount,
        t4.mint AS token_d_mint,
        t4.amount AS token_d_amount
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
    LEFT JOIN
        transfers AS t3
        ON t3.block_timestamp::date = b.block_timestamp::date
        AND t3.tx_id = b.tx_id
        AND t3.index = b.index
        AND coalesce(t3.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t3.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND b.token_c_account IS NOT NULL
        AND t3.dest_token_account = b.token_c_account
        AND t3.source_token_account = b.pool_token_c_account
    LEFT JOIN
        transfers AS t4
        ON t4.block_timestamp::date = b.block_timestamp::date
        AND t4.tx_id = b.tx_id
        AND t4.index = b.index
        AND coalesce(t4.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t4.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND b.token_d_account IS NOT NULL
        AND t4.dest_token_account = b.token_d_account
        AND t4.source_token_account = b.pool_token_d_account
    WHERE
        b.event_type = 'removeLiquidity'
    QUALIFY
        row_number() OVER (PARTITION BY b.tx_id, b.index, b.inner_index ORDER BY t.inner_index, t2.inner_index, t3.inner_index, t4.inner_index) = 1
),

single_withdraw_transfers AS (
    SELECT 
        b.*,
        t.mint AS token_a_mint,
        t.amount AS token_a_amount,
        NULL AS token_b_mint,
        NULL AS token_b_amount,
        NULL AS token_c_mint,
        NULL AS token_c_amount,
        NULL AS token_d_mint,
        NULL AS token_d_amount
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
        AND t.source_token_account IN (b.pool_token_a_account, b.pool_token_b_account, b.pool_token_c_account, b.pool_token_d_account)
    WHERE
        b.event_type = 'removeLiquidityOneToken'
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
        token_c_mint,
        token_c_amount,
        token_d_mint,
        token_d_amount,
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
        token_c_mint,
        token_c_amount,
        token_d_mint,
        token_d_amount,
        program_id,
        _inserted_timestamp
    FROM 
        withdraw_transfers

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
        token_c_mint,
        token_c_amount,
        token_d_mint,
        token_d_amount,
        program_id,
        _inserted_timestamp
    FROM 
        single_withdraw_transfers
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
    token_a_mint,
    token_a_amount,
    token_b_mint,
    token_b_amount,
    token_c_mint,
    token_c_amount,
    token_d_mint,
    token_d_amount,
    program_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_meteora_multi_2_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    pre_final