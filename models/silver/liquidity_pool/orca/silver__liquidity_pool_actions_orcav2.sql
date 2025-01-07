-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['block_timestamp::date','liquidity_pool_actions_orcav2_id'],
        incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
        merge_exclude_columns = ["inserted_timestamp"],
        cluster_by = ['block_timestamp::date','modified_timestamp::date'],
        post_hook = enable_search_optimization(
            '{{this.schema}}',
            '{{this.identifier}}',
            'ON EQUALITY(tx_id, provider_address, token_a_mint, token_b_mint, liquidity_pool_actions_orcav2_id)'
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
    CREATE OR REPLACE TEMPORARY TABLE silver.liquidity_pool_actions_orcav2__intermediate_tmp AS 
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
        program_id = '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
        AND event_type IN (
            'depositAllTokenTypes', 
            'depositSingleTokenTypeExactAmountIn', 
            'withdrawAllTokenTypes', 
            'withdrawSingleTokenTypeExactAmountOut'
        )
        {% if is_incremental() %}
        AND _inserted_timestamp > '{{ max_timestamp }}'
        {% else %}
        AND block_timestamp::date between '2021-06-10' and '2023-01-01'
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.liquidity_pool_actions_orcav2__intermediate_tmp","block_timestamp::date") %}
{% endif %}

with base as (
    select 
        * exclude(accounts),
        silver.udf_get_account_pubkey_by_name('swap', accounts) AS pool_address,
        silver.udf_get_account_pubkey_by_name('userTransferAuthority', accounts) AS provider_address,
        silver.udf_get_account_pubkey_by_name('swapTokenA', accounts) AS pool_token_a_account,
        silver.udf_get_account_pubkey_by_name('swapTokenB', accounts) AS pool_token_b_account,
        case 
            when event_type = 'depositAllTokenTypes' then
                silver.udf_get_account_pubkey_by_name('depositTokenA', accounts)
            when event_type = 'depositSingleTokenTypeExactAmountIn' then
                silver.udf_get_account_pubkey_by_name('sourceToken', accounts)
            when event_type = 'withdrawAllTokenTypes' then
                silver.udf_get_account_pubkey_by_name('destinationTokenA', accounts)
            when event_type = 'withdrawSingleTokenTypeExactAmountOut' then
                silver.udf_get_account_pubkey_by_name('destination', accounts)
            else
                NULL
        end AS token_a_account,
        case 
            when event_type = 'depositAllTokenTypes' then
                silver.udf_get_account_pubkey_by_name('depositTokenB', accounts)
            when event_type = 'withdrawAllTokenTypes' then
                silver.udf_get_account_pubkey_by_name('destinationTokenB', accounts)
            else
                NULL
        end AS token_b_account,
        coalesce(lead(inner_index) over (partition by tx_id, index order by inner_index),9999) AS next_lp_action_inner_index
    from 
        silver.liquidity_pool_actions_orcav2__intermediate_tmp
),
transfers as (
    select * exclude(index),
        split_part(index,'.',1)::int AS index,
        nullif(split_part(index,'.',2),'')::int AS inner_index
    from
        {{ ref('silver__transfers') }}
    WHERE
        {{ between_stmts }}
),
deposit_transfers AS (
    select 
        b.*,
        t.mint AS token_a_mint,
        t.amount AS token_a_amount,
        t2.mint AS token_b_mint,
        t2.amount AS token_b_amount,
    from base AS b
    left join
        transfers AS t
        ON t.block_timestamp::date = b.block_timestamp::date
        AND t.tx_id = b.tx_id
        AND t.index = b.index
        AND coalesce(t.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND t.source_token_account = b.token_a_account
        AND t.dest_token_account = b.pool_token_a_account
        -- AND (
        --         (t.source_token_account = b.token_a_account
        --         AND t.dest_token_account = b.pool_token_a_account)
        --     OR
        --         (t.source_token_account = b.token_a_account
        --         AND t.dest_token_account = b.pool_token_b_account
        --         AND b.event_type = 'depositSingleTokenTypeExactAmountIn') /* we always represent single token deposit as a deposit of "token A" */
        --     )
    left join
        transfers AS t2
        ON t2.block_timestamp::date = b.block_timestamp::date
        AND t2.tx_id = b.tx_id
        AND t2.index = b.index
        AND coalesce(t2.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t2.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND t2.source_token_account = b.token_b_account
        AND t2.dest_token_account = b.pool_token_b_account
    where
        b.event_type IN (
            'depositAllTokenTypes'
        )
),
single_deposit_transfers AS (
    select 
        b.*,
        t.mint AS token_a_mint,
        t.amount AS token_a_amount,
        NULL AS token_b_mint,
        NULL AS token_b_amount,
    from base AS b
    left join
        transfers AS t
        ON t.block_timestamp::date = b.block_timestamp::date
        AND t.tx_id = b.tx_id
        AND t.index = b.index
        AND coalesce(t.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND t.source_token_account = b.token_a_account
        AND t.dest_token_account IN (b.pool_token_a_account, b.pool_token_b_account)
    where
        b.event_type = 'depositSingleTokenTypeExactAmountIn'
),
withdraw_transfers AS (
    select 
        b.*,
        t.mint AS token_a_mint,
        t.amount AS token_a_amount,
        t2.mint AS token_b_mint,
        t2.amount AS token_b_amount,
    from base AS b
    left join
        transfers AS t
        ON t.block_timestamp::date = b.block_timestamp::date
        AND t.tx_id = b.tx_id
        AND t.index = b.index
        AND coalesce(t.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND t.dest_token_account = b.token_a_account
        AND t.source_token_account = b.pool_token_a_account
    left join
        transfers AS t2
        ON t2.block_timestamp::date = b.block_timestamp::date
        AND t2.tx_id = b.tx_id
        AND t2.index = b.index
        AND coalesce(t2.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t2.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND t2.dest_token_account = b.token_b_account
        AND t2.source_token_account = b.pool_token_b_account
    where
        b.event_type = 'withdrawAllTokenTypes'
),
single_withdraw_transfers AS (
    select 
        b.*,
        t.mint AS token_a_mint,
        t.amount AS token_a_amount,
        NULL AS token_b_mint,
        NULL AS token_b_amount,
    from base AS b
    left join
        transfers AS t
        ON t.block_timestamp::date = b.block_timestamp::date
        AND t.tx_id = b.tx_id
        AND t.index = b.index
        AND coalesce(t.inner_index,0) > coalesce(b.inner_index,-1)
        AND coalesce(t.inner_index,0) < coalesce(b.next_lp_action_inner_index,9999)
        AND t.dest_token_account = b.token_a_account
        AND t.source_token_account IN (b.pool_token_a_account, b.pool_token_b_account)
    where
        b.event_type = 'withdrawSingleTokenTypeExactAmountOut'
)
select
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
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_orcav2_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from deposit_transfers
union all
select 
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
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_orcav2_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from withdraw_transfers
union all
select 
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
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_orcav2_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from single_deposit_transfers
union all
select 
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
    {{ dbt_utils.generate_surrogate_key(['block_id', 'tx_id', 'index', 'inner_index']) }} AS liquidity_pool_actions_orcav2_id,
    sysdate() as inserted_timestamp,
    sysdate() as modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from single_withdraw_transfers