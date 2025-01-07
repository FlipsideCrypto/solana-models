-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = 'initialization_pools_orcav1_id',
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
    CREATE OR REPLACE TEMPORARY TABLE silver.initialization_pools_orcav1__intermediate_tmp AS 
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
        program_id = 'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'
        AND event_type = 'initialize'
        {% if is_incremental() %}
        AND _inserted_timestamp > '{{ max_timestamp }}'
        {% endif %}
    {% endset %}
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate("silver.initialization_pools_orcav1__intermediate_tmp","block_timestamp::date") %}
{% endif %}

WITH base AS (
    SELECT
        * exclude(accounts),
        silver.udf_get_account_pubkey_by_name('swap', accounts) AS pool_address,
        silver.udf_get_account_pubkey_by_name('pool', accounts) AS pool_token_mint,
        silver.udf_get_account_pubkey_by_name('tokenA', accounts) AS token_a_account,
        silver.udf_get_account_pubkey_by_name('tokenB', accounts) AS token_b_account
    FROM
        silver.initialization_pools_orcav1__intermediate_tmp
),
token_account_mints AS (
    SELECT
        tx_id,
        index,
        inner_index,
        token_a_account,
        {{ target.database }}.live.udf_api(
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
                    token_a_account,
                    OBJECT_CONSTRUCT(
                        'encoding',
                        'jsonParsed'
                    )
                )
            ),
            'Vault/prod/solana/quicknode/mainnet'
        ) AS token_a_response,
        token_b_account,
        {{ target.database }}.live.udf_api(
            'POST',
            '{service}/{Authentication}',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),
            OBJECT_CONSTRUCT(
                'id',
                2,
                'jsonrpc',
                '2.0',
                'method',
                'getAccountInfo',
                'params',
                ARRAY_CONSTRUCT(
                    token_b_account,
                    OBJECT_CONSTRUCT(
                        'encoding',
                        'jsonParsed'
                    )
                )
            ),
            'Vault/prod/solana/quicknode/mainnet'
        ) AS token_b_response
    FROM
        base
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
    token_a_response:data:result:value:data:parsed:info:mint::string AS token_a_mint,
    b.token_b_account,
    token_b_response:data:result:value:data:parsed:info:mint::string AS token_b_mint,
    b.program_id,
    b._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['b.block_id', 'b.tx_id', 'b.index', 'b.inner_index']) }} AS initialization_pools_orcav1_id,
    sysdate() AS inserted_timestamp,
    sysdate() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    base AS b
JOIN
    token_account_mints AS t
    ON b.tx_id = t.tx_id
    AND b.index = t.index
    AND coalesce(b.inner_index, -1) = coalesce(t.inner_index, -1)
