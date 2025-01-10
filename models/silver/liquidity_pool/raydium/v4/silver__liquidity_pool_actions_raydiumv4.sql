-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['block_timestamp::date','liquidity_pool_actions_raydiumv4_id'],
        incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
        merge_exclude_columns = ["inserted_timestamp"],
        cluster_by = ['block_timestamp::date','modified_timestamp::date'],
        post_hook = enable_search_optimization(
            '{{this.schema}}',
            '{{this.identifier}}',
            'ON EQUALITY(tx_id, provider_address, token_a_mint, token_b_mint, liquidity_pool_actions_raydiumv4_id)'
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
    CREATE OR REPLACE TEMPORARY TABLE silver.liquidity_pool_actions_raydiumv4__intermediate_tmp AS 
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
        program_id = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
        AND event_type IN (
            'deposit',
            'withdraw',
            'withdrawPnl'
        )
        AND succeeded
        {% if is_incremental() %}
        -- AND _inserted_timestamp > '{{ max_timestamp }}'
        /* batches for reload */
        -- AND block_timestamp::date BETWEEN '2022-01-01' AND '2022-06-01'
        -- AND block_timestamp::date BETWEEN '2022-06-01' AND '2023-01-01'
        AND block_timestamp::date BETWEEN '2023-01-01' AND '2023-06-01'
        -- AND block_timestamp::date BETWEEN '2023-06-01' AND '2024-01-01'
        -- AND block_timestamp::date BETWEEN '2024-01-01' AND '2024-06-01'
        -- AND block_timestamp::date BETWEEN '2024-06-01' AND '2025-01-05'
        -- AND _inserted_timestamp > '{{ max_timestamp }}'::timestamp_ntz - INTERVAL '1 DAY'
        {% else %}
        -- AND block_timestamp::date BETWEEN '2024-01-01' AND '2024-02-05'
        AND block_timestamp::date BETWEEN '2021-03-21' AND '2022-01-01'
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.liquidity_pool_actions_raydiumv4__intermediate_tmp","block_timestamp::date") %}
{% endif %}

{% set bad_withdraw_pnl_accounts_cutoff_date = '2023-01-04' %}

WITH base AS (
    SELECT 
        * exclude(accounts),
        silver.udf_get_account_pubkey_by_name('amm', accounts) AS pool_address,
        CASE
            WHEN event_type IN ('deposit', 'withdraw') THEN
                silver.udf_get_account_pubkey_by_name('userOwner', accounts)
            WHEN event_type = 'withdrawPnl' AND block_timestamp >= '{{ bad_withdraw_pnl_accounts_cutoff_date }}' THEN
                silver.udf_get_account_pubkey_by_name('pnlOwnerAccount', accounts)
            WHEN event_type = 'withdrawPnl' AND block_timestamp < '{{ bad_withdraw_pnl_accounts_cutoff_date }}' THEN
                silver.udf_get_account_pubkey_by_name('pcPnlTokenAccount', accounts)
        END AS provider_address,
        CASE
            WHEN event_type = 'withdrawPnl' AND block_timestamp < '{{ bad_withdraw_pnl_accounts_cutoff_date }}' THEN
                silver.udf_get_account_pubkey_by_name('ammOpenOrders', accounts) 
            ELSE
                silver.udf_get_account_pubkey_by_name('poolCoinTokenAccount', accounts) 
        END AS pool_token_a_account,
        CASE
            WHEN event_type = 'withdrawPnl' AND block_timestamp < '{{ bad_withdraw_pnl_accounts_cutoff_date }}' THEN
                silver.udf_get_account_pubkey_by_name('poolCoinTokenAccount', accounts) 
            ELSE
                silver.udf_get_account_pubkey_by_name('poolPcTokenAccount', accounts) 
        END AS pool_token_b_account,
        CASE 
            WHEN event_type = 'deposit' THEN
                silver.udf_get_account_pubkey_by_name('userCoinTokenAccount', accounts)
            WHEN event_type = 'withdraw' THEN
                silver.udf_get_account_pubkey_by_name('uerCoinTokenAccount', accounts)
            WHEN event_type = 'withdrawPnl' AND block_timestamp >= '{{ bad_withdraw_pnl_accounts_cutoff_date }}' THEN
                silver.udf_get_account_pubkey_by_name('coinPnlTokenAccount', accounts)
            WHEN event_type = 'withdrawPnl' AND block_timestamp < '{{ bad_withdraw_pnl_accounts_cutoff_date }}' THEN
                silver.udf_get_account_pubkey_by_name('poolPcTokenAccount', accounts)
            ELSE NULL
        END AS token_a_account,
        CASE 
            WHEN event_type = 'deposit' THEN
                silver.udf_get_account_pubkey_by_name('userPcTokenAccount', accounts)
            WHEN event_type = 'withdraw' THEN
                silver.udf_get_account_pubkey_by_name('uerPcTokenAccount', accounts)
            WHEN event_type = 'withdrawPnl' AND block_timestamp >= '{{ bad_withdraw_pnl_accounts_cutoff_date }}' THEN
                silver.udf_get_account_pubkey_by_name('pcPnlTokenAccount', accounts)
            WHEN event_type = 'withdrawPnl' AND block_timestamp < '{{ bad_withdraw_pnl_accounts_cutoff_date }}' THEN
                silver.udf_get_account_pubkey_by_name('coinPnlTokenAccount', accounts)
            ELSE NULL
        END AS token_b_account,
        coalesce(lead(inner_index) OVER (
            PARTITION BY tx_id, index 
            ORDER BY inner_index
        ), 9999) AS next_lp_action_inner_index
    FROM 
        silver.liquidity_pool_actions_raydiumv4__intermediate_tmp
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
        b.event_type = 'deposit'
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
        b.event_type IN ('withdraw', 'withdrawPnl')
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
        _inserted_timestamp,
        {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_raydiumv4_id,
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
        {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_raydiumv4_id,
        sysdate() AS inserted_timestamp,
        sysdate() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM 
        withdraw_transfers
)
SELECT
    *
FROM
    pre_final
WHERE
    /*
    4 withdraw transactions from 2021 with bad instruction accounts data. 
    Omitting from model
    */
    (
        block_timestamp > '2021-05-24 08:55:09.000'
        OR (
            block_timestamp <= '2021-05-24 08:55:09.000'
            AND token_a_amount IS NOT NULL
            AND token_b_amount IS NOT NULL
            AND token_a_mint IS NOT NULL
            AND token_b_mint IS NOT NULL
        )
    )
