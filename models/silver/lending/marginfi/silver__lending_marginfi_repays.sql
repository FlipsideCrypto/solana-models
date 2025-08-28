-- depends_on: {{ ref('silver__decoded_instructions_combined') }}

{{ config(
    materialized = 'incremental',
    unique_key = ['lending_marginfi_repays_id'],
    incremental_predicates = ["dynamic_range_predicate", "block_timestamp::date"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE', 'modified_timestamp::DATE'],
    tags = ['scheduled_non_core']
) }}

{% if execute %}
    {% set base_query %}
        CREATE OR REPLACE TEMPORARY TABLE silver.lending_marginfi_repays__tmp AS
        SELECT
            *
        FROM
            {{ ref('silver__decoded_instructions_combined') }}
        WHERE
            program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'
            AND event_type = 'lendingAccountRepay'
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
        "silver.lending_marginfi_repays__tmp",
        "block_timestamp::date"
    ) %}
{% endif %}

WITH base AS (
    SELECT
        *
    FROM
        silver.lending_marginfi_repays__tmp
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
        silver.udf_get_account_pubkey_by_name('signer', decoded_instruction:accounts) AS payer,
        -- BORROWER (owner of the debt being repaid - same as payer in MarginFi)
        silver.udf_get_account_pubkey_by_name('signer', decoded_instruction:accounts) AS borrower,
        silver.udf_get_account_pubkey_by_name('bank', decoded_instruction:accounts) AS bank,
        decoded_instruction:args:amount::NUMBER AS amount_raw,
        _inserted_timestamp
    FROM
        base
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
    a.payer,
    a.borrower,
    b.token_address,
    a.amount_raw,
    a.amount_raw * POW(10, -b.decimal) AS amount,
    b.decimal,
    a._inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id', 'a.index', 'COALESCE(a.inner_index, -1)']) }} AS lending_marginfi_repays_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    decoded a
LEFT JOIN {{ ref('silver__lending_marginfi_banks') }} b
    ON a.bank = b.bank