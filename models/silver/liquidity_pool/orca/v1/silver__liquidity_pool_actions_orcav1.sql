-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['block_timestamp::date','liquidity_pool_actions_orcav1_id'],
        incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
        merge_exclude_columns = ["inserted_timestamp"],
        cluster_by = ['block_timestamp::date','modified_timestamp::date'],
        post_hook = enable_search_optimization(
            '{{this.schema}}',
            '{{this.identifier}}',
            'ON EQUALITY(tx_id, provider_address, token_a_mint, token_b_mint, liquidity_pool_actions_orcav1_id)'
        ),
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
    CREATE OR REPLACE TEMPORARY TABLE silver.liquidity_pool_actions_orcav1__intermediate_tmp AS 
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        index,
        inner_index,
        succeeded,
        event_type,
        decoded_instruction:accounts AS accounts,
        program_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined')}}
    WHERE
        program_id = 'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'
        AND event_type IN (
            'depositAllTokenTypes', 
            'depositSingleTokenTypeExactAmountIn', 
            'withdrawAllTokenTypes', 
            'withdrawSingleTokenTypeExactAmountOut'
        )
        {% if is_incremental() %}
        -- AND _inserted_timestamp > '{{ max_timestamp }}'
        AND block_timestamp::date BETWEEN '2022-06-01'  AND '2023-01-01'
        {% else %}
        /* 
        there are data issues from 2021-02-14 to 2021-03-17 with transfers 
        and some deposits that don't seem to have transfers at all.
        It would take significant time and complexity for solve for these edge cases 
        so we are electing to exclude this data for now 
        examples:
            -- 7Jdcs7rwoC3n8vSX6B3CdB98Ms8zzU7AwtAPrNYPuma2tZCdJTWGiEmC5w63RPpWNpfxZFvHrq72baExvU1FGEc
            -- 34x5eT8bpjKFr2hZzRQLQ77jyLy4KMH98WWeNgJiFmS4q8pECVXkg3iKSd9e5mnehj5kQa37F1XRGWKh795DAH1f
        */
        AND block_timestamp::date BETWEEN '2021-03-17' AND '2022-06-01' 
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.liquidity_pool_actions_orcav1__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT 
        * exclude(accounts),
        silver.udf_get_account_pubkey_by_name('swap', accounts) AS pool_address,
        silver.udf_get_account_pubkey_by_name('userTransferAuthority', accounts) AS provider_address,
        silver.udf_get_account_pubkey_by_name('swapTokenA', accounts) AS pool_token_a_account,
        silver.udf_get_account_pubkey_by_name('swapTokenB', accounts) AS pool_token_b_account,
        CASE 
            WHEN event_type = 'depositAllTokenTypes' THEN
                silver.udf_get_account_pubkey_by_name('depositTokenA', accounts)
            WHEN event_type = 'depositSingleTokenTypeExactAmountIn' THEN
                silver.udf_get_account_pubkey_by_name('sourceToken', accounts)
            WHEN event_type = 'withdrawAllTokenTypes' THEN
                silver.udf_get_account_pubkey_by_name('destinationTokenA', accounts)
            WHEN event_type = 'withdrawSingleTokenTypeExactAmountOut' THEN
                silver.udf_get_account_pubkey_by_name('destination', accounts)
            ELSE NULL
        END AS token_a_account,
        CASE 
            WHEN event_type = 'depositAllTokenTypes' THEN
                silver.udf_get_account_pubkey_by_name('depositTokenB', accounts)
            WHEN event_type = 'withdrawAllTokenTypes' THEN
                silver.udf_get_account_pubkey_by_name('destinationTokenB', accounts)
            ELSE NULL
        END AS token_b_account,
        coalesce(lead(inner_index) OVER (
            PARTITION BY tx_id, index 
            ORDER BY inner_index
        ), 9999) AS next_lp_action_inner_index
    FROM 
        silver.liquidity_pool_actions_orcav1__intermediate_tmp
),

transfers AS (
    SELECT 
        * exclude(index),
        split_part(index,'.',1)::int AS index,
        nullif(split_part(index,'.',2),'')::int AS inner_index
    FROM
        {{ ref('silver__transfers') }}
    WHERE
        {{ between_stmts }}
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
        b.event_type = 'depositAllTokenTypes'
),

single_deposit_transfers AS (
    SELECT 
        b.*,
        t.mint AS token_a_mint,
        t.amount AS token_a_amount,
        NULL AS token_b_mint,
        NULL AS token_b_amount
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
        AND t.dest_token_account IN (b.pool_token_a_account, b.pool_token_b_account)
    WHERE
        b.event_type = 'depositSingleTokenTypeExactAmountIn'
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
        b.event_type = 'withdrawAllTokenTypes'
),

single_withdraw_transfers AS (
    SELECT 
        b.*,
        t.mint AS token_a_mint,
        t.amount AS token_a_amount,
        NULL AS token_b_mint,
        NULL AS token_b_amount
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
        AND t.source_token_account IN (b.pool_token_a_account, b.pool_token_b_account)
    WHERE
        b.event_type = 'withdrawSingleTokenTypeExactAmountOut'
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
    program_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_orcav1_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
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
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_orcav1_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
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
    program_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_orcav1_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    single_deposit_transfers

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
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_orcav1_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    single_withdraw_transfers
