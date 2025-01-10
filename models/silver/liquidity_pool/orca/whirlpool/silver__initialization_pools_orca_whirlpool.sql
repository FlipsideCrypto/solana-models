-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['block_timestamp::DATE','initialization_pools_orca_whirlpool_id'],
        incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
        merge_exclude_columns = ["inserted_timestamp"],
        cluster_by = ['block_timestamp::DATE','modified_timestamp::DATE'],
        tags = ['scheduled_non_core'],
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
    CREATE OR REPLACE TEMPORARY TABLE silver.initialization_pools_orca_whirlpool__intermediate_tmp AS 
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
        program_id = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'
        AND event_type IN ('initializePool','initializePoolV2')
        AND succeeded
        {% if is_incremental() %}
        AND _inserted_timestamp > '{{ max_timestamp }}'
        {% else %}
        AND _inserted_timestamp BETWEEN '2023-11-14' AND '2025-01-01'
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.initialization_pools_orca_whirlpool__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT
        * exclude(accounts),
        silver.udf_get_account_pubkey_by_name('whirlpool', accounts) AS pool_address,
        silver.udf_get_account_pubkey_by_name('tokenVaultA', accounts) AS token_a_account,
        silver.udf_get_account_pubkey_by_name('tokenVaultB', accounts) AS token_b_account,
        silver.udf_get_account_pubkey_by_name('tokenMintA', accounts) AS token_a_mint,
        silver.udf_get_account_pubkey_by_name('tokenMintB', accounts) AS token_b_mint
    FROM
        silver.initialization_pools_orca_whirlpool__intermediate_tmp
)
SELECT 
    b.block_id,
    b.block_timestamp,
    b.tx_id,
    b.index,
    b.inner_index,
    b.succeeded,
    b.pool_address,
    NULL AS pool_token_mint, -- Whirlpool does not have LP mints
    b.token_a_account,
    b.token_a_mint,
    b.token_b_account,
    b.token_b_mint,
    b.program_id,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['b.block_id', 'b.tx_id', 'b.index', 'b.inner_index']) }} AS initialization_pools_orca_whirlpool_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    base AS b