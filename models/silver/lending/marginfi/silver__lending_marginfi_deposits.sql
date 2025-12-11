-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['lending_marginfi_deposits_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.lending_marginfi_deposits__tmp AS
        SELECT
            *
        FROM
            {{ ref('silver__decoded_instructions_combined') }}
        WHERE
            program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'
            AND event_type = 'lendingAccountDeposit'
            AND succeeded
        {% if is_incremental() %}
            AND _inserted_timestamp >= (
                SELECT
                    MAX(_inserted_timestamp) - INTERVAL '1 hour'
                FROM
                    {{ this }}
            )
        {% else %}
            AND _inserted_timestamp::DATE >= '2024-01-10'
        {% endif %}
    {% endset %}
    
    {% do run_query(base_query) %}
    {% set between_stmts = fsc_utils.dynamic_range_predicate(
        "silver.lending_marginfi_deposits__tmp",
        "block_timestamp::date"
    ) %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.lending_marginfi_deposits__tmp
),

decoded AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        inner_index,
        program_id,
        event_type,
        silver.udf_get_account_pubkey_by_name('signer', decoded_instruction:accounts) AS depositor,
        silver.udf_get_account_pubkey_by_name('bank', decoded_instruction:accounts) AS bank,
        silver.udf_get_account_pubkey_by_name('signerTokenAccount', decoded_instruction:accounts) AS signer_token_account,
        silver.udf_get_account_pubkey_by_name('bankLiquidityVault', decoded_instruction:accounts) AS bank_liquidity_vault,
        decoded_instruction:args:amount::INT AS amount_raw,
        _inserted_timestamp
    FROM
        base
    WHERE
        amount_raw > 0
),

transfers AS (
    SELECT
        A.*,
        COALESCE(SPLIT_PART(INDEX :: text, '.', 1) :: INT, INDEX :: INT) AS index_1,
        NULLIF(SPLIT_PART(INDEX :: text, '.', 2), '') :: INT AS inner_index_1
    FROM
        {{ ref('silver__transfers') }} A
        INNER JOIN (
            SELECT
                DISTINCT tx_id,
                    block_timestamp::DATE AS block_date
            FROM
                decoded
        ) d
        ON d.block_date = A.block_timestamp::DATE
        AND d.tx_id = A.tx_id
    WHERE
        A.succeeded
        AND {{ between_stmts }}
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
)

SELECT 
    a.block_timestamp,
    a.block_id,
    a.tx_id,
    a.index,
    a.inner_index,
    a.program_id,
    a.event_type,
    a.bank,
    a.depositor,
    b.mint AS token_address,
    a.bank_liquidity_vault,
    a.amount_raw,
    b.amount,
    c.decimal,
    a._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id', 'a.index', 'COALESCE(a.inner_index, -1)']) }} AS lending_marginfi_deposits_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    decoded a
LEFT JOIN transfers b
    ON a.tx_id = b.tx_id 
    AND a.signer_token_account = b.source_token_account
    AND a.bank_liquidity_vault = b.dest_token_account
    AND a.index = b.index_1
    and coalesce(a.inner_index,-1) < b.inner_index_1
LEFT JOIN token_decimals c
    ON b.mint = c.mint
