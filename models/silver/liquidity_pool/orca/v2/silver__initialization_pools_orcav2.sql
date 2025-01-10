-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'initialization_pools_orcav2_id',
        incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
        merge_exclude_columns = ["inserted_timestamp"],
        cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
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
    CREATE OR REPLACE TEMPORARY TABLE silver.initialization_pools_orcav2__intermediate_tmp AS 
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        index,
        inner_index,
        succeeded,
        decoded_instruction:accounts AS accounts,
        program_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined')}}
    WHERE
        program_id = '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP'
        AND event_type = 'initialize'
        {% if is_incremental() %}
        AND _inserted_timestamp > '{{ max_timestamp }}'
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.initialization_pools_orcav2__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT
        * exclude(accounts),
        silver.udf_get_account_pubkey_by_name('swap', accounts) AS pool_address,
        silver.udf_get_account_pubkey_by_name('pool', accounts) AS pool_token_mint,
        silver.udf_get_account_pubkey_by_name('tokenA', accounts) AS token_a_account,
        silver.udf_get_account_pubkey_by_name('tokenB', accounts) AS token_b_account
    FROM
        silver.initialization_pools_orcav2__intermediate_tmp
),
post_token_balances AS (
    SELECT
        block_timestamp,
        tx_id,
        account,
        mint
    FROM
        {{ ref('silver___post_token_balances') }}
    WHERE
        {{ between_stmts }}
)
SELECT 
    b.block_id,
    b.block_timestamp,
    b.tx_id,
    b.index,
    b.inner_index,
    b.succeeded,
    b.pool_address,
    b.pool_token_mint,
    b.token_a_account,
    ptb_a.mint AS token_a_mint,
    b.token_b_account,
    ptb_b.mint AS token_b_mint,
    b.program_id,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['b.block_id', 'b.tx_id', 'b.index', 'b.inner_index']) }} AS initialization_pools_orcav2_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    base AS b
LEFT JOIN
    post_token_balances AS ptb_a
    ON b.block_timestamp::date = ptb_a.block_timestamp::date
    AND b.tx_id = ptb_a.tx_id
    AND b.token_a_account = ptb_a.account
LEFT JOIN
    post_token_balances AS ptb_b
    ON b.block_timestamp::date = ptb_b.block_timestamp::date
    AND b.tx_id = ptb_b.tx_id
    AND b.token_b_account = ptb_b.account