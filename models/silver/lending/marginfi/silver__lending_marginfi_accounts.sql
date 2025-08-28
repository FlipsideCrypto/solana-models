-- depends_on: {{ ref('silver__decoded_instructions_combined') }}
{{ config(
    materialized = 'incremental',
    unique_key = ['lending_marginfi_accounts_id'],
    tags = ['scheduled_non_core']
) }}
WITH marginfi_accounts AS (
    SELECT DISTINCT
        tx_id,
        block_timestamp,
        silver.udf_get_account_pubkey_by_name('marginfiAccount', decoded_instruction:accounts) AS marginfi_account,
        silver.udf_get_account_pubkey_by_name('authority', decoded_instruction:accounts) AS owner,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_instructions_combined') }}
    WHERE
        program_id = 'MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA'
        AND event_type = 'marginfiAccountInitialize'
        {% if is_incremental() %}
            AND _inserted_timestamp >= (
                SELECT
                    MAX(_inserted_timestamp)
                FROM
                    {{ this }}
            )
        {% else %}
            AND _inserted_timestamp::DATE >= '2024-01-10'
        {% endif %}
)

SELECT 
    tx_id,
    block_timestamp,
    marginfi_account,
    owner,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(['marginfi_account']) }} AS lending_marginfi_accounts_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    marginfi_accounts
