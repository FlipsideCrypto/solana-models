-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['block_timestamp::DATE','initialization_pools_raydiumstable_id'],
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
    CREATE OR REPLACE TEMPORARY TABLE silver.initialization_pools_raydiumstable__intermediate_tmp AS 
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
        program_id = '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h'
        AND event_type = 'initialize'
        AND succeeded
        {% if is_incremental() %}
        AND _inserted_timestamp > '{{ max_timestamp }}'
        {% else %}
        AND _inserted_timestamp BETWEEN '2024-05-24' AND '2024-05-28'
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.initialization_pools_raydiumstable__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT
        * exclude(accounts),
        silver.udf_get_account_pubkey_by_name('ammOpenOrders', accounts) AS pool_address,
        silver.udf_get_account_pubkey_by_name('pcMint', accounts) AS pool_token_mint,
        silver.udf_get_account_pubkey_by_name('withdrawQueue', accounts) AS token_a_account,
        silver.udf_get_account_pubkey_by_name('tokenDestLp', accounts) AS token_b_account,
        silver.udf_get_account_pubkey_by_name('poolTokenCoin', accounts) AS token_a_mint,
        silver.udf_get_account_pubkey_by_name('poolTokenPc', accounts) AS token_b_mint
    FROM
        silver.initialization_pools_raydiumstable__intermediate_tmp
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
    b.token_a_mint,
    b.token_b_account,
    b.token_b_mint,
    b.program_id,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['b.block_id', 'b.tx_id', 'b.index', 'b.inner_index']) }} AS initialization_pools_raydiumstable_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    base AS b
WHERE
    /* the USDT/USDC pool was created twice according to instructions, ignoring the first one */
    b.block_timestamp > '2022-01-18 07:37:50.000'
